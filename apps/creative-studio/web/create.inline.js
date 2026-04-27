
    const statusEl = document.getElementById('workspace-status');
    const inlineStatusEl = document.getElementById('workspace-inline-status');
    const taskListEl = document.getElementById('task-list');
    const taskSummaryEl = document.getElementById('task-summary');
    const materialListEl = document.getElementById('material-list');
    const targetListEl = document.getElementById('target-list');
    const targetFormEl = document.getElementById('target-form');
    const resultPanelEl = document.getElementById('result-panel');
    const generatePacketBtnEl = document.getElementById('generate-packet-btn');
    const targetSaveBtnEl = document.getElementById('target-save-btn');
    const autofillBtnEl = document.getElementById('target-autofill-btn');
    const query = new URLSearchParams(window.location.search);

    const state = {
      tasks: [],
      targets: [],
      activeTaskId: query.get('task_id') || '',
      activeTargetId: '',
      bundle: null,
      latestPacket: null,
      articleLookup: new Map(),
    };

    async function api(path, options = {}) {
      const init = { method: options.method || 'GET', headers: options.headers || {} };
      if (options.body !== undefined) {
        init.body = typeof options.body === 'string' ? options.body : JSON.stringify(options.body);
        init.headers['Content-Type'] = 'application/json';
      }
      const response = await fetch(path, init);
      const text = await response.text();
      let payload = {};
      try {
        payload = text ? JSON.parse(text) : {};
      } catch (error) {
        throw new Error(text || '接口返回了无法解析的内容');
      }
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.message || '请求失败');
      }
      return payload;
    }

    function escapeHtml(text) {
      return String(text || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function dedupe(items) {
      return Array.from(new Set((items || []).map((item) => String(item || '').trim()).filter(Boolean)));
    }

    function splitLines(value) {
      return String(value || '')
        .split(/\r?\n/)
        .map((item) => item.trim())
        .filter(Boolean);
    }

    function formatTime(value) {
      if (!value) return '未记录';
      const parsed = new Date(String(value).replace(' ', 'T'));
      if (!Number.isNaN(parsed.getTime())) {
        return new Intl.DateTimeFormat('zh-CN', {
          timeZone: 'Asia/Shanghai',
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: false,
        }).format(parsed).replace(/\//g, '-');
      }
      return String(value).replace('T', ' ').replace('+00:00', '');
    }

    function setStatus(message, tone = 'default') {
      if (!message) {
        statusEl.style.display = 'none';
        statusEl.textContent = '';
        return;
      }
      statusEl.style.display = 'block';
      statusEl.style.borderColor = tone === 'error'
        ? 'rgba(184, 75, 90, 0.25)'
        : tone === 'success'
          ? 'rgba(45, 118, 87, 0.22)'
          : 'rgba(23, 48, 66, 0.1)';
      statusEl.style.background = tone === 'error'
        ? 'rgba(184, 75, 90, 0.08)'
        : tone === 'success'
          ? 'rgba(45, 118, 87, 0.08)'
          : 'rgba(255, 255, 255, 0.84)';
      statusEl.style.color = tone === 'error'
        ? '#9b3d49'
        : tone === 'success'
          ? '#21563f'
          : 'var(--muted)';
      statusEl.textContent = message;
    }

    function setInlineStatus(message, tone = 'default', options = {}) {
      if (!message) {
        inlineStatusEl.style.display = 'none';
        inlineStatusEl.innerHTML = '';
        return;
      }
      inlineStatusEl.style.display = 'block';
      inlineStatusEl.style.borderColor = tone === 'error'
        ? 'rgba(184, 75, 90, 0.25)'
        : tone === 'success'
          ? 'rgba(45, 118, 87, 0.22)'
          : 'rgba(23, 48, 66, 0.1)';
      inlineStatusEl.style.background = tone === 'error'
        ? 'rgba(184, 75, 90, 0.08)'
        : tone === 'success'
          ? 'rgba(45, 118, 87, 0.08)'
          : 'rgba(255, 255, 255, 0.84)';
      inlineStatusEl.style.color = tone === 'error'
        ? '#9b3d49'
        : tone === 'success'
          ? '#21563f'
          : 'var(--muted)';
      const actionHtml = options.actionLabel
        ? `<div class="button-row" style="margin-top: 10px;"><button id="inline-status-action-btn" type="button" class="secondary">${escapeHtml(options.actionLabel)}</button></div>`
        : '';
      inlineStatusEl.innerHTML = `<div>${escapeHtml(message)}</div>${actionHtml}`;
      if (options.actionLabel && typeof options.onAction === 'function') {
        document.getElementById('inline-status-action-btn')?.addEventListener('click', options.onAction);
      }
    }

    function setButtonBusy(button, busy, idleText, busyText) {
      if (!button) return;
      button.disabled = Boolean(busy);
      button.dataset.busy = busy ? '1' : '0';
      button.textContent = busy ? busyText : idleText;
    }

    function getGenerateButtonIdleText() {
      const target = getActiveTarget();
      if (target?.ui_preset === 'khazix_longform') return '生成文章';
      if (target?.ui_preset === 'voiceover_script') return '生成口播稿';
      return '生成下游创作包';
    }

    function updateGenerateButtonLabel() {
      if (generatePacketBtnEl.dataset.busy === '1') return;
      generatePacketBtnEl.textContent = getGenerateButtonIdleText();
    }

    function importedTasks(tasks) {
      const seen = new Set();
      const filtered = (tasks || []).filter((task) => {
        const trigger = String(task.trigger_type || '').trim();
        const metadata = task.metadata || {};
        const sourcePacket = metadata.source_packet || {};
        const packetPreview = metadata.packet_preview || {};
        const creationPreview = metadata.creation_packet_preview || {};
        const isImported = Boolean(
          sourcePacket.packet_id
          || sourcePacket.raw_item_ids
          || sourcePacket.packet_kind
          || ['topic_packet', 'event_packet', 'nighthawk_sources'].includes(trigger)
        );
        if (!isImported) return false;
        if (sourcePacket.packet_type === 'topic' && Number(packetPreview.total_results || 0) <= 0) return false;
        if (sourcePacket.source_type === 'nighthawk_raw_items' && Number(creationPreview.selected_count || 0) <= 0) return false;
        const dedupeKey = sourcePacket.packet_id
          || `${sourcePacket.source_type || trigger}:${JSON.stringify(sourcePacket.raw_item_ids || [])}`
          || `${trigger}:${task.topic || ''}`;
        if (seen.has(dedupeKey)) return false;
        seen.add(dedupeKey);
        return true;
      });
      return filtered.length ? filtered : (tasks || []);
    }

    function getActiveTarget() {
      if (!state.targets.length) return null;
      return state.targets.find((item) => item.id === state.activeTargetId) || state.targets[0];
    }

    function inferTargetId(bundle) {
      const task = bundle?.task || {};
      const metadata = task.metadata || {};
      const preferredSkill = metadata.preferred_writer_skill || bundle?.writer_jobs?.[0]?.writer_skill || '';
      if (metadata.creation_target_id) {
        return metadata.creation_target_id;
      }
      const matched = state.targets.find((item) => String(item.writer_skill || '').trim() === String(preferredSkill || '').trim());
      return matched ? matched.id : (state.targets[0]?.id || '');
    }

    function getImportedMaterials(bundle) {
      const results = Array.isArray(bundle?.retrieval_batch?.results) ? bundle.retrieval_batch.results : [];
      const kept = results.filter((item) => {
        const decision = String(item.decision || '').trim().toLowerCase();
        const classification = String(item.classification || '').trim().toLowerCase();
        return ['keep', 'primary', 'supporting'].includes(decision) || ['primary', 'secondary', 'supporting'].includes(classification);
      });
      return kept.length ? kept : results;
    }

    function materialNeedsBodyFetch(item) {
      const raw = item?.raw || {};
      const rawBody = String(raw.content || raw.transcript_text || raw.transcript_excerpt || '').trim();
      const resolvedText = String(item?.text || item?.summary || '').trim();
      const recommendFullFetch = String(item?.recommend_full_fetch || '').trim().toLowerCase();
      if (recommendFullFetch === 'yes' || recommendFullFetch === 'body_missing') {
        return !rawBody || raw.body_fetch_ok === false;
      }
      return !rawBody && !resolvedText && Boolean(item?.url);
    }

    function materialCanFetchBody(item) {
      return materialNeedsBodyFetch(item) && Boolean(String(item?.url || '').trim());
    }

    function getMaterialBody(item) {
      return item?.raw?.content || item?.text || item?.summary || '暂无可展示正文';
    }

    function renderTaskList() {
      const tasks = importedTasks(state.tasks);
      if (!tasks.length) {
        taskListEl.innerHTML = `
          <div class="empty">
            还没有导入好的素材。先去 <a href="/create/topic">主题搜索台</a> 或 <a href="/create/nighthawk">正文资料池</a> 选择材料，再回到这里继续编排。
          </div>
        `;
        return;
      }

      taskListEl.innerHTML = tasks.map((task) => {
        const active = task.id === state.activeTaskId;
        const sourceType = task.metadata?.source_packet?.packet_type || task.trigger_type || 'imported';
        return `
          <article class="task-card ${active ? 'active' : ''}" data-task-id="${escapeHtml(task.id)}">
            <div class="task-card-header">
              <strong>${escapeHtml(task.topic || '未命名任务')}</strong>
              <button class="task-delete-btn" data-task-delete="${escapeHtml(task.id)}" type="button">删除</button>
            </div>
            <div class="muted">${escapeHtml(task.angle || task.goal || '等待配置创作方向')}</div>
            <div class="meta-line">
              <span>${escapeHtml(sourceType)}</span>
              <span>${escapeHtml(formatTime(task.updated_at || task.created_at))}</span>
            </div>
          </article>
        `;
      }).join('');

      taskListEl.querySelectorAll('[data-task-id]').forEach((node) => {
        node.addEventListener('click', () => selectTask(node.dataset.taskId || ''));
      });
      taskListEl.querySelectorAll('[data-task-delete]').forEach((node) => {
        node.addEventListener('click', async (event) => {
          event.stopPropagation();
          await deleteTask(node.dataset.taskDelete || '');
        });
      });
    }

    function renderTaskSummary() {
      const bundle = state.bundle;
      if (!bundle?.task) {
        taskSummaryEl.innerHTML = '<div class="empty">请先从上方选择一个素材包。</div>';
        return;
      }
      const task = bundle.task;
      const metadata = task.metadata || {};
      const materials = getImportedMaterials(bundle);
      const primaryCount = materials.filter((item) => String(item.classification || '').trim().toLowerCase() === 'primary').length;
      const sourceLabel = metadata.source_packet?.packet_type || task.trigger_type || 'imported';

      taskSummaryEl.innerHTML = `
        <h2 class="section-title">${escapeHtml(task.topic || '未命名任务')}</h2>
        <p class="muted">${escapeHtml(task.goal || '这里承接已经导入好的素材，再继续生成下游创作包。')}</p>
        <div class="summary-grid" style="margin-top: 16px;">
          <div class="summary-card">
            <strong>${materials.length}</strong>
            <div class="muted">当前导入文章数</div>
          </div>
          <div class="summary-card">
            <strong>${primaryCount}</strong>
            <div class="muted">主资料数</div>
          </div>
          <div class="summary-card">
            <strong>${escapeHtml(sourceLabel)}</strong>
            <div class="muted">导入来源</div>
          </div>
        </div>
        <div class="pill-row" style="margin-top: 16px;">
          <span class="pill">平台 ${escapeHtml(task.platform || '未设置')}</span>
          <span class="pill">受众 ${escapeHtml(task.audience || '未设置')}</span>
          <span class="pill">最近更新 ${escapeHtml(formatTime(task.updated_at || task.created_at))}</span>
        </div>
      `;
    }

    async function excludeMaterial(sourceId) {
      if (!state.bundle?.task?.id || !state.bundle?.retrieval_batch?.id) return;
      try {
        setInlineStatus('正在从当前素材包移出这篇文章...', 'default');
        await api(`/api/create/tasks/${encodeURIComponent(state.bundle.task.id)}/retrieval/${encodeURIComponent(state.bundle.retrieval_batch.id)}/exclude`, {
          method: 'POST',
          body: {
            source_id: sourceId,
            reason: '用户从编排台手动移出素材包',
          },
        });
        await loadBundle(state.bundle.task.id, false);
        setInlineStatus('已从当前素材包移出这篇文章。', 'success');
      } catch (error) {
        setInlineStatus(`移出素材失败：${error.message}`, 'error');
      }
    }

    async function fetchMaterialBody(sourceId) {
      if (!state.bundle?.task?.id || !state.bundle?.retrieval_batch?.id) return;
      try {
        setInlineStatus('正在为这篇素材发起抓正文任务...', 'default');
        const payload = await api(`/api/create/tasks/${encodeURIComponent(state.bundle.task.id)}/retrieval/${encodeURIComponent(state.bundle.retrieval_batch.id)}/${encodeURIComponent(sourceId)}/fetch-body`, {
          method: 'POST',
          body: {
            retry_count: 1,
            analyze: true,
            save_to_db: true,
          },
        });
        const title = payload.title || payload.source_id || '当前素材';
        setInlineStatus(`已发起抓正文：${title}。完成后刷新当前任务即可。`, 'success');
      } catch (error) {
        setInlineStatus(`抓取正文失败：${error.message}`, 'error');
      }
    }

    function renderMaterials() {
      const materials = getImportedMaterials(state.bundle);
      state.articleLookup = new Map(materials.map((item) => [String(item.source_id || ''), item]));
      if (!materials.length) {
        materialListEl.innerHTML = '<div class="empty">当前任务还没有可直接使用的文章。</div>';
        return;
      }

      materialListEl.innerHTML = materials.map((item) => {
        const sourceId = String(item.source_id || '');
        const needsBodyFetch = materialNeedsBodyFetch(item);
        const canFetchBody = materialCanFetchBody(item);
        return `
          <article class="material-card clickable ${needsBodyFetch ? 'body-missing' : ''}" data-source-id="${escapeHtml(sourceId)}">
            <div class="material-topline">
              <strong>${escapeHtml(item.title || item.source_id || '未命名文章')}</strong>
              <div class="material-badges">
                ${needsBodyFetch ? '<span class="material-badge warning">正文未抓取</span>' : '<span class="material-badge">正文已就绪</span>'}
              </div>
            </div>
            <div class="muted">${escapeHtml(item.summary || item.why_pick || '暂无摘要')}</div>
            <div class="meta-line">
              <span>${escapeHtml(item.source || item.channel || '未知来源')}</span>
              <span>${escapeHtml(item.classification || '未分层')}</span>
              <span>${escapeHtml(formatTime(item.published_at || item.created_at))}</span>
            </div>
            <div class="button-row material-actions">
              ${needsBodyFetch ? `<button type="button" class="secondary" data-material-fetch="${escapeHtml(sourceId)}" ${canFetchBody ? '' : 'disabled'}>抓取正文</button>` : ''}
              <button type="button" class="secondary" data-material-exclude="${escapeHtml(sourceId)}">移出素材包</button>
            </div>
          </article>
        `;
      }).join('');

      materialListEl.querySelectorAll('[data-source-id]').forEach((node) => {
        node.addEventListener('click', () => openArticleModal(node.dataset.sourceId || ''));
      });
      materialListEl.querySelectorAll('[data-material-exclude]').forEach((node) => {
        node.addEventListener('click', async (event) => {
          event.stopPropagation();
          await excludeMaterial(node.dataset.materialExclude || '');
        });
      });
      materialListEl.querySelectorAll('[data-material-fetch]').forEach((node) => {
        node.addEventListener('click', async (event) => {
          event.stopPropagation();
          if (node.hasAttribute('disabled')) return;
          await fetchMaterialBody(node.dataset.materialFetch || '');
        });
      });
    }

    function renderTargetCards() {
      if (!state.targets.length) {
        targetListEl.innerHTML = '<div class="empty">创作模式配置尚未加载。</div>';
        return;
      }
      targetListEl.innerHTML = state.targets.map((target) => `
        <article class="target-card ${target.id === state.activeTargetId ? 'active' : ''}" data-target-id="${escapeHtml(target.id)}">
          <strong>${escapeHtml(target.label || target.id)}</strong>
          <div class="target-desc">${escapeHtml(target.description || '')}</div>
          <div class="meta-line">
            <span>${escapeHtml(target.writer_skill || 'generic-longform')}</span>
            ${target.is_default ? '<span>默认</span>' : ''}
          </div>
        </article>
      `).join('');

      targetListEl.querySelectorAll('[data-target-id]').forEach((node) => {
        node.addEventListener('click', () => {
          state.activeTargetId = node.dataset.targetId || '';
          renderTargetCards();
          renderTargetForm();
          updateGenerateButtonLabel();
        });
      });
    }

    function renderChoiceGroup(name, items, selectedItems) {
      return `
        <div class="choice-group">
          ${(items || []).map((item) => {
            const value = String(item.value || '');
            const selected = selectedItems.includes(value);
            return `
              <label class="choice">
                <input type="checkbox" name="${escapeHtml(name)}" value="${escapeHtml(value)}" ${selected ? 'checked' : ''} />
                <span>${escapeHtml(item.label || value)}</span>
              </label>
            `;
          }).join('')}
        </div>
      `;
    }

    function getStoredTargetInputs(bundle) {
      return bundle?.task?.metadata?.creation_target_inputs || {};
    }

    function renderKhazixForm(target, bundle, defaults, writerJob) {
      const task = bundle.task;
      const storedInputs = getStoredTargetInputs(bundle);
      const currentArchetype = writerJob.article_archetype || defaults.article_archetype || '';
      const voiceNotes = dedupe(writerJob.user_voice_notes || task.style_notes || []);
      const bannedPatterns = dedupe(writerJob.banned_patterns || task.banned_patterns || []);
      const angle = task.angle || '';
      const topicReason = storedInputs.topic_reason || '';
      const openingHook = storedInputs.opening_hook || '';
      const followups = dedupe(writerJob.optional_followups || defaults.optional_followups || []);
      const articleOptions = (target.article_archetypes || []).map((item) => `
        <option value="${escapeHtml(item.value)}" ${item.value === currentArchetype ? 'selected' : ''}>${escapeHtml(item.label || item.value)}</option>
      `).join('');

      return `
        <div class="field-grid">
          <div class="field">
            <label for="field-article-archetype">文章原型</label>
            <select id="field-article-archetype" name="article_archetype">${articleOptions}</select>
          </div>
          <div class="field">
            <label>附带输出</label>
            ${renderChoiceGroup('optional_followups', target.optional_followup_options || [], followups)}
          </div>
          <div class="field span-2">
            <label for="field-topic-reason">这篇为什么值得写</label>
            <textarea id="field-topic-reason" name="topic_reason" placeholder="用 2-4 句话说明这题为什么值得现在写。">${escapeHtml(topicReason)}</textarea>
          </div>
          <div class="field span-2">
            <label for="field-angle">核心判断与切口</label>
            <textarea id="field-angle" name="angle" placeholder="这次真正想立住的判断是什么。">${escapeHtml(angle)}</textarea>
          </div>
          <div class="field span-2">
            <label for="field-opening-hook">开头气口</label>
            <textarea id="field-opening-hook" name="opening_hook" placeholder="写一句想让文章开头就打到人的话。">${escapeHtml(openingHook)}</textarea>
          </div>
          <div class="field span-2">
            <label for="field-voice-notes">表达要求</label>
            <textarea id="field-voice-notes" name="user_voice_notes" placeholder="每行一个要求。">${escapeHtml(voiceNotes.join('\n'))}</textarea>
          </div>
          <div class="field span-2">
            <label for="field-banned-patterns">避免写法</label>
            <textarea id="field-banned-patterns" name="banned_patterns" placeholder="每行一个不希望出现的写法。">${escapeHtml(bannedPatterns.join('\n'))}</textarea>
          </div>
        </div>
      `;
    }

    function renderCompactTargetForm(target, bundle, defaults, writerJob) {
      const task = bundle.task;
      const storedInputs = getStoredTargetInputs(bundle);
      const currentArchetype = writerJob.article_archetype || defaults.article_archetype || '';
      const voiceNotes = dedupe(writerJob.user_voice_notes || task.style_notes || []);
      const bannedPatterns = dedupe(writerJob.banned_patterns || task.banned_patterns || []);
      const angle = task.angle || '';
      const followups = dedupe(writerJob.optional_followups || defaults.optional_followups || []);
      const articleOptions = (target.article_archetypes || []).map((item) => `
        <option value="${escapeHtml(item.value)}" ${item.value === currentArchetype ? 'selected' : ''}>${escapeHtml(item.label || item.value)}</option>
      `).join('');

      return `
        <div class="field-grid">
          <div class="field">
            <label for="field-article-archetype">内容类型</label>
            <select id="field-article-archetype" name="article_archetype">${articleOptions}</select>
          </div>
          <div class="field">
            <label>附带输出</label>
            ${renderChoiceGroup('optional_followups', target.optional_followup_options || [], followups)}
          </div>
          <div class="field span-2">
            <label for="field-angle">核心切角</label>
            <textarea id="field-angle" name="angle" placeholder="${escapeHtml((target.fields || []).find((item) => item.key === 'angle')?.placeholder || '这次想先讲透什么')}">${escapeHtml(angle)}</textarea>
          </div>
          <div class="field span-2">
            <label for="field-topic-reason">为什么要做这条</label>
            <textarea id="field-topic-reason" name="topic_reason" placeholder="一句到几句，说明这条内容为什么值得现在做。">${escapeHtml(storedInputs.topic_reason || '')}</textarea>
          </div>
          <div class="field span-2">
            <label for="field-voice-notes">表达要求</label>
            <textarea id="field-voice-notes" name="user_voice_notes" placeholder="每行一个表达要求。">${escapeHtml(voiceNotes.join('\n'))}</textarea>
          </div>
          <div class="field span-2">
            <label for="field-banned-patterns">避免写法</label>
            <textarea id="field-banned-patterns" name="banned_patterns" placeholder="每行一个不希望出现的写法。">${escapeHtml(bannedPatterns.join('\n'))}</textarea>
          </div>
        </div>
      `;
    }

    function renderTargetForm() {
      const target = getActiveTarget();
      const bundle = state.bundle;
      if (!target || !bundle?.task) {
        targetFormEl.innerHTML = '<div class="empty">请先选择素材包，再选择创作模式。</div>';
        return;
      }

      const writerJob = bundle.writer_jobs?.[bundle.writer_jobs.length - 1] || {};
      const defaults = target.defaults || {};
      const primaryLabel = ((target.primary_output_options || []).find((item) => item.value === (defaults.primary_output || [])[0]) || {}).label
        || ((defaults.primary_output || [])[0] || '未设置');
      const intro = target.ui_preset === 'khazix_longform'
        ? 'Khazix 长文模式只保留真正影响出稿质量的核心输入。素材包里的文章已经加载好，你现在只需要确定文章原型、切口、开头气口和表达边界。'
        : '当前模式只展示真正会影响下游输出的必要输入。';
      const autofillMeta = bundle.task?.metadata?.creation_target_autofill || {};
      const autofillEnabled = target.ui_preset === 'khazix_longform';
      const autofillReady = autofillEnabled && autofillMeta.target_id === target.id && Boolean(autofillMeta.generated_at);

      autofillBtnEl.style.display = autofillEnabled ? 'inline-flex' : 'none';
      autofillBtnEl.disabled = !autofillEnabled;
      autofillBtnEl.textContent = autofillReady ? '重新智能填写' : '智能填写';

      const autofillNote = autofillReady
        ? `<div class="muted" style="margin-top: 8px;">已基于 ${escapeHtml(String(autofillMeta.material_count || 0))} 篇素材生成一版建议，最近更新 ${escapeHtml(formatTime(autofillMeta.generated_at))}，来源 ${escapeHtml(autofillMeta.source || 'mock')}</div>`
        : (autofillEnabled
          ? '<div class="muted" style="margin-top: 8px;">点击“智能填写”，会基于当前素材包里的文章，自动生成一版建议。</div>'
          : '');

      targetFormEl.innerHTML = `
        <div class="result-block" style="margin-bottom: 12px;">
          <div class="pill-row">
            <span class="pill">默认主产物：${escapeHtml(primaryLabel)}</span>
            <span class="pill">下游技能：${escapeHtml(target.writer_skill || 'generic-longform')}</span>
          </div>
          <div class="muted" style="margin-top: 10px;">${escapeHtml(intro)}</div>
          ${autofillNote}
        </div>
        ${target.ui_preset === 'khazix_longform'
          ? renderKhazixForm(target, bundle, defaults, writerJob)
          : renderCompactTargetForm(target, bundle, defaults, writerJob)}
      `;
      updateGenerateButtonLabel();
    }

    function collectTargetPayload() {
      const target = getActiveTarget();
      const readField = (name) => String(targetFormEl.querySelector(`[name="${name}"]`)?.value || '').trim();
      const optionalFollowups = targetFormEl.querySelectorAll('input[name="optional_followups"]:checked');
      const hkrFocus = targetFormEl.querySelectorAll('input[name="hkr_focus"]:checked');
      const defaultPrimaryOutput = Array.isArray(target?.defaults?.primary_output) ? target.defaults.primary_output : [];
      return {
        creation_target_id: state.activeTargetId,
        preferred_writer_skill: target?.writer_skill || 'generic-longform',
        writer_skill: target?.writer_skill || 'generic-longform',
        content_template: target?.defaults?.content_template || '',
        angle: readField('angle'),
        topic_reason: readField('topic_reason'),
        opening_hook: readField('opening_hook'),
        personal_observations: readField('personal_observations'),
        hkr_focus: Array.from(hkrFocus).map((item) => item.value),
        article_archetype: readField('article_archetype'),
        primary_output: defaultPrimaryOutput,
        optional_followups: Array.from(optionalFollowups).map((item) => item.value),
        user_voice_notes: splitLines(readField('user_voice_notes')),
        banned_patterns: splitLines(readField('banned_patterns')),
      };
    }

    async function saveCurrentTarget(silent = false) {
      if (!state.bundle?.task?.id) return;
      const task = state.bundle.task;
      const payload = collectTargetPayload();
      const metadata = {
        ...(task.metadata || {}),
        preferred_writer_skill: payload.preferred_writer_skill,
        creation_target_id: state.activeTargetId,
      };
      await api(`/api/create/tasks/${encodeURIComponent(task.id)}`, {
        method: 'POST',
        body: {
          angle: payload.angle,
          topic_reason: payload.topic_reason,
          opening_hook: payload.opening_hook,
          personal_observations: payload.personal_observations,
          hkr_focus: payload.hkr_focus,
          creation_target_id: payload.creation_target_id,
          preferred_writer_skill: payload.preferred_writer_skill,
          style_notes: payload.user_voice_notes,
          banned_patterns: payload.banned_patterns,
          metadata,
        },
      });
      if (!silent) {
        setStatus('当前创作模式已保存。', 'success');
      }
      await loadBundle(task.id, false);
    }

    async function triggerPrimaryBodyFetch() {
      if (!state.bundle?.task?.id) return;
      const taskId = state.bundle.task.id;
      try {
        setInlineStatus('正在发起主资料补正文任务...', 'default');
        const payload = await api(`/api/create/tasks/${encodeURIComponent(taskId)}/citations/fetch-primary-body`, {
          method: 'POST',
          body: {
            retry_count: 1,
            analyze: true,
            save_to_db: true,
          },
        });
        const title = payload.title || payload.source_id || '当前素材';
        setInlineStatus(`已发起补正文：${title}。补完后再点一次“生成下游创作包”即可。`, 'success');
      } catch (error) {
        setInlineStatus(`补正文失败：${error.message}`, 'error');
      }
    }

    function renderResultPanel() {
      const latestWriterJob = state.bundle?.writer_jobs?.[state.bundle.writer_jobs.length - 1];
      const latestDraft = state.bundle?.article_drafts?.[0];
      if (!state.bundle?.task) {
        resultPanelEl.innerHTML = '<div class="empty">请先选择素材包。</div>';
        return;
      }
      if (latestDraft) {
        resultPanelEl.innerHTML = `
          <div class="result-block">
            <div class="pill-row" style="margin-bottom: 12px;">
              <span class="pill">已生成文章</span>
              <span class="pill">${escapeHtml(latestDraft.writer_skill || 'writer')}</span>
              <span class="pill">最近更新：${escapeHtml(formatTime(latestDraft.updated_at || latestDraft.created_at))}</span>
            </div>
            <div class="field-grid">
              <div class="field span-2">
                <label>文章标题</label>
                <div>${escapeHtml(latestDraft.title || '未命名文章')}</div>
              </div>
              <div class="field span-2">
                <label>质检概览</label>
                <div>${escapeHtml(latestDraft.quality_report?.summary || '暂无质检结果')}</div>
              </div>
            </div>
            <div class="button-row" style="margin-top: 14px;">
              <a class="secondary" href="/create/write?draft_id=${encodeURIComponent(latestDraft.id || '')}">打开文章编辑台</a>
            </div>
          </div>
        `;
        return;
      }
      if (!latestWriterJob) {
        resultPanelEl.innerHTML = '<div class="empty">还没有生成内容。</div>';
        return;
      }

      const packetPayload = state.latestPacket || {};
      const packet = packetPayload.packet || packetPayload;
      const writerBrief = packet.writer_ready_brief || {};
      const outputs = (latestWriterJob.primary_output || []).join('、') || '未设置';
      const followups = (latestWriterJob.optional_followups || []).join('、') || '无';

      resultPanelEl.innerHTML = `
        <div class="result-block">
          <div class="pill-row" style="margin-bottom: 12px;">
            <span class="pill">主产物：${escapeHtml(outputs)}</span>
            <span class="pill">附带输出：${escapeHtml(followups)}</span>
            <span class="pill">最近生成：${escapeHtml(formatTime(latestWriterJob.updated_at || latestWriterJob.created_at))}</span>
          </div>
          <div class="field-grid">
            <div class="field">
              <label>文章原型</label>
              <div>${escapeHtml(latestWriterJob.article_archetype || '未设置')}</div>
            </div>
            <div class="field">
              <label>创作包路径</label>
              <div>${escapeHtml(latestWriterJob.packet_path || '')}</div>
            </div>
            <div class="field span-2">
              <label>为什么值得写</label>
              <div>${escapeHtml(writerBrief.why_this_topic || state.bundle.task.metadata?.creation_target_inputs?.topic_reason || '尚未填写')}</div>
            </div>
            <div class="field span-2">
              <label>核心判断</label>
              <div>${escapeHtml(writerBrief.core_judgement || state.bundle.task.angle || '尚未填写')}</div>
            </div>
            <div class="field span-2">
              <label>开头气口</label>
              <div>${escapeHtml(writerBrief.opening_hook || state.bundle.task.metadata?.creation_target_inputs?.opening_hook || '尚未生成')}</div>
            </div>
          </div>
        </div>
      `;
    }

    async function generatePacketFlow() {
      if (generatePacketBtnEl.dataset.busy === '1') return;
      if (!state.bundle?.task?.id) {
        setInlineStatus('请先选择一个素材包，再生成。', 'error');
        return;
      }
      const taskId = state.bundle.task.id;
      const idleText = getGenerateButtonIdleText();
      const target = getActiveTarget();
      setButtonBusy(generatePacketBtnEl, true, idleText, '生成中...');
      try {
        setInlineStatus(target?.ui_preset === 'khazix_longform' ? '正在调用写作器生成文章，请稍等...' : '正在生成，请稍等...');
        setStatus(target?.ui_preset === 'khazix_longform' ? '正在整理素材、应用 Khazix Writer 规则，并生成可编辑文章...' : '正在整理已导入素材，并生成适配当前模式的内容...');
        const payload = collectTargetPayload();
        await saveCurrentTarget(true);
        if (target?.ui_preset === 'khazix_longform') {
          const draftPayload = await api(`/api/create/tasks/${encodeURIComponent(taskId)}/article/generate`, {
            method: 'POST',
            body: payload,
          });
          const nextUrl = draftPayload.next_url || `/create/write?draft_id=${encodeURIComponent(draftPayload.article_draft?.id || '')}`;
          setInlineStatus('文章已生成，正在打开编辑页。', 'success');
          window.location.href = nextUrl;
          return;
        }
        await api(`/api/create/tasks/${encodeURIComponent(taskId)}/bootstrap-packet-flow`, {
          method: 'POST',
          body: {
            angle: payload.angle,
            opening_hook: payload.opening_hook,
            content_template: payload.content_template,
          },
        });
        const writerJob = await api(`/api/create/tasks/${encodeURIComponent(taskId)}/writer-job/generate`, {
          method: 'POST',
          body: payload,
        });
        await loadBundle(taskId, false);
        try {
          const writerPacketPayload = await api(`/api/create/tasks/${encodeURIComponent(taskId)}/writer-packet`);
          state.latestPacket = writerPacketPayload.packet || writerPacketPayload;
        } catch (error) {
          state.latestPacket = null;
        }
        renderResultPanel();
        setInlineStatus('已生成新的下游创作包。结果已经刷新到下方。', 'success');
        setStatus('已经生成新的下游创作包，当前模式为 ' + (getActiveTarget()?.label || writerJob.writer_job?.writer_skill || '未命名模式') + '。', 'success');
      } catch (error) {
        const message = error.message || '生成失败';
        if (message.includes('Gate 3')) {
          setInlineStatus(message, 'error', {
            actionLabel: '补主资料正文',
            onAction: () => {
              triggerPrimaryBodyFetch();
            },
          });
        } else {
          setInlineStatus(message, 'error');
        }
        setStatus('生成失败：' + message, 'error');
      } finally {
        setButtonBusy(generatePacketBtnEl, false, getGenerateButtonIdleText(), '生成中...');
      }
    }

    window.generatePacketFlow = generatePacketFlow;

    async function runAutofill() {
      if (!state.bundle?.task?.id) return;
      const target = getActiveTarget();
      if (!target || target.ui_preset !== 'khazix_longform') return;
      try {
        setStatus('正在基于当前素材包做智能填写...');
        const payload = await api(`/api/create/tasks/${encodeURIComponent(state.bundle.task.id)}/autofill`, {
          method: 'POST',
          body: {
            creation_target_id: target.id,
          },
        });
        state.bundle = payload.bundle || state.bundle;
        state.activeTargetId = inferTargetId(state.bundle);
        renderTaskSummary();
        renderMaterials();
        renderTargetCards();
        renderTargetForm();
        renderResultPanel();
        setStatus('智能填写已完成，已基于 ' + (payload.autofill?.material_count || 0) + ' 篇素材生成一版建议。', 'success');
      } catch (error) {
        setStatus('智能填写失败：' + error.message, 'error');
      }
    }

    async function loadTargets() {
      const summary = await api('/api/create/targets');
      state.targets = summary.targets || [];
    }

    async function loadBundle(taskId, updateHistory = true) {
      const bundle = await api(`/api/create/tasks/${encodeURIComponent(taskId)}`);
      state.bundle = bundle;
      state.activeTaskId = taskId;
      state.activeTargetId = inferTargetId(bundle);
      setInlineStatus('');
      if (updateHistory) {
        const next = new URL(window.location.href);
        next.searchParams.set('task_id', taskId);
        window.history.replaceState({}, '', next.toString());
      }
      renderTaskList();
      renderTaskSummary();
      renderMaterials();
      renderTargetCards();
      renderTargetForm();
      try {
        const writerPacketPayload = await api(`/api/create/tasks/${encodeURIComponent(taskId)}/writer-packet`);
        state.latestPacket = writerPacketPayload.packet || writerPacketPayload;
      } catch (error) {
        state.latestPacket = null;
      }
      renderResultPanel();
      setStatus('');
    }

    async function selectTask(taskId, updateHistory = true) {
      if (!taskId) return;
      try {
        await loadBundle(taskId, updateHistory);
      } catch (error) {
        state.tasks = (state.tasks || []).filter((item) => item.id !== taskId);
        renderTaskList();
        setStatus('加载任务失败：' + error.message, 'error');
      }
    }

    async function loadTasks() {
      const payload = await api('/api/create/tasks?limit=30');
      state.tasks = payload.items || [];
      renderTaskList();
      const preferredTaskId = state.activeTaskId || importedTasks(state.tasks)[0]?.id || state.tasks[0]?.id || '';
      if (preferredTaskId) {
        await selectTask(preferredTaskId, false);
      } else {
        state.bundle = null;
        renderTaskSummary();
        renderMaterials();
        renderTargetCards();
        renderTargetForm();
        renderResultPanel();
        setStatus('还没有导入好的素材。先去主题搜索台或正文资料池选一批文章，再回到这里继续编排。');
      }
    }

    async function deleteTask(taskId) {
      const confirmed = window.confirm('确认删除这个素材包吗？它对应的编排数据和已生成创作包也会一起移除。');
      if (!confirmed) return;
      try {
        await api(`/api/create/tasks/${encodeURIComponent(taskId)}/delete`, { method: 'POST', body: {} });
        state.tasks = state.tasks.filter((item) => item.id !== taskId);
        if (state.activeTaskId === taskId) {
          state.activeTaskId = '';
          state.bundle = null;
          state.latestPacket = null;
        }
        await loadTasks();
        setStatus('素材包已删除。', 'success');
      } catch (error) {
        setStatus('删除失败：' + error.message, 'error');
      }
    }

    function openArticleModal(sourceId) {
      const article = state.articleLookup.get(String(sourceId || ''));
      if (!article) return;
      document.getElementById('article-modal-title').textContent = article.title || article.source_id || '未命名文章';
      document.getElementById('article-modal-meta').innerHTML = `
        <span>${escapeHtml(article.source || article.channel || '未知来源')}</span>
        <span>${escapeHtml(article.classification || '未分层')}</span>
        <span>${escapeHtml(formatTime(article.published_at || article.created_at))}</span>
        <span>${escapeHtml(article.url || '无外链')}</span>
      `;
      document.getElementById('article-modal-body').innerHTML = `<pre>${escapeHtml(getMaterialBody(article))}</pre>`;
      document.getElementById('article-modal').classList.add('open');
      document.getElementById('article-modal').setAttribute('aria-hidden', 'false');
    }

    function closeArticleModal() {
      document.getElementById('article-modal').classList.remove('open');
      document.getElementById('article-modal').setAttribute('aria-hidden', 'true');
    }

    targetSaveBtnEl.addEventListener('click', () => saveCurrentTarget(false));
    generatePacketBtnEl.addEventListener('click', generatePacketFlow);
    autofillBtnEl.addEventListener('click', runAutofill);
    document.getElementById('article-modal-close').addEventListener('click', closeArticleModal);
    document.getElementById('article-modal').addEventListener('click', (event) => {
      if (event.target.id === 'article-modal') closeArticleModal();
    });

    (async () => {
      try {
        setStatus('正在加载编排台...', 'default');
        await loadTargets();
        await loadTasks();
      } catch (error) {
        setStatus('加载失败：' + error.message, 'error');
      }
    })();
  
