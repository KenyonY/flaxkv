// FlaxKV2 Inspector Web UI - JavaScript

// 全局状态
const state = {
    currentPage: 1,
    pageSize: 50,
    totalKeys: 0,
    currentPattern: null,
    selectedKey: null,
    dbInfo: {}
};

// API 基础 URL
const API_BASE = '';

// ========== 工具函数 ==========

// 显示通知
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = `toast show ${type}`;
    setTimeout(() => {
        toast.className = 'toast';
    }, 3000);
}

// 格式化大小
function formatSize(bytes) {
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex++;
    }
    return `${size.toFixed(2)} ${units[unitIndex]}`;
}

// 格式化 JSON
function formatJSON(obj) {
    try {
        if (typeof obj === 'string') {
            obj = JSON.parse(obj);
        }
        return JSON.stringify(obj, null, 2);
    } catch (e) {
        return obj;
    }
}

// ========== API 调用 ==========

// 获取数据库信息
async function fetchDBInfo() {
    try {
        const response = await fetch(`${API_BASE}/api/info`);
        const data = await response.json();
        state.dbInfo = data;
        updateDBInfo();
    } catch (error) {
        showToast('获取数据库信息失败', 'error');
    }
}

// 获取键列表
async function fetchKeys(pattern = null, offset = 0) {
    try {
        let url = `${API_BASE}/api/keys?limit=${state.pageSize}&offset=${offset}`;
        if (pattern) {
            url += `&pattern=${encodeURIComponent(pattern)}`;
        }

        const response = await fetch(url);
        const result = await response.json();

        if (result.success) {
            state.totalKeys = result.data.total;
            renderKeyList(result.data.keys);
            updatePagination();
        } else {
            showToast(result.error, 'error');
        }
    } catch (error) {
        showToast('获取键列表失败', 'error');
    }
}

// 获取键详情
async function fetchKeyDetail(key) {
    try {
        const response = await fetch(`${API_BASE}/api/keys/${encodeURIComponent(key)}`);
        const result = await response.json();

        if (result.success) {
            renderKeyDetail(result.data);
        } else {
            showToast(result.error, 'error');
        }
    } catch (error) {
        showToast('获取键详情失败', 'error');
    }
}

// 获取统计信息
async function fetchStats() {
    try {
        const statsContainer = document.getElementById('stats-container');
        statsContainer.innerHTML = '<p class="loading">加载中</p>';

        const response = await fetch(`${API_BASE}/api/stats`);
        const result = await response.json();

        if (result.success) {
            renderStats(result.data);
        } else {
            showToast(result.error, 'error');
        }
    } catch (error) {
        showToast('获取统计信息失败', 'error');
    }
}

// 搜索键
async function searchKeys(pattern) {
    try {
        const response = await fetch(
            `${API_BASE}/api/search?pattern=${encodeURIComponent(pattern)}&limit=100`
        );
        const result = await response.json();

        if (result.success) {
            const keys = result.data.results.map(r => r.key);
            renderKeyList(keys);
            showToast(`找到 ${keys.length} 个匹配的键`, 'success');
        } else {
            showToast(result.error, 'error');
        }
    } catch (error) {
        showToast('搜索失败', 'error');
    }
}

// 删除键
async function deleteKey(key) {
    try {
        const response = await fetch(`${API_BASE}/api/keys/${encodeURIComponent(key)}`, {
            method: 'DELETE'
        });
        const result = await response.json();

        if (result.success) {
            showToast('删除成功', 'success');
            refreshKeyList();
        } else {
            showToast(result.error, 'error');
        }
    } catch (error) {
        showToast('删除失败', 'error');
    }
}

// 设置键值
async function setKey(key, value, ttl = null) {
    try {
        const response = await fetch(`${API_BASE}/api/keys`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ key, value, ttl })
        });
        const result = await response.json();

        if (result.success) {
            showToast('保存成功', 'success');
            refreshKeyList();
        } else {
            showToast(result.error, 'error');
        }
    } catch (error) {
        showToast('保存失败', 'error');
    }
}

// ========== 渲染函数 ==========

// 更新数据库信息
function updateDBInfo() {
    document.getElementById('db-name').textContent = `数据库: ${state.dbInfo.db_name}`;
    document.getElementById('db-path').textContent = `路径: ${state.dbInfo.path}`;
}

// 渲染键列表
function renderKeyList(keys) {
    const container = document.getElementById('key-list-container');

    if (!keys || keys.length === 0) {
        container.innerHTML = '<p class="placeholder">未找到任何键</p>';
        return;
    }

    container.innerHTML = keys.map(key => `
        <div class="key-item" data-key="${key}">
            <span class="key-name">${key}</span>
        </div>
    `).join('');

    // 添加点击事件
    container.querySelectorAll('.key-item').forEach(item => {
        item.addEventListener('click', () => {
            // 移除其他选中状态
            container.querySelectorAll('.key-item').forEach(i => i.classList.remove('active'));
            // 添加当前选中状态
            item.classList.add('active');
            // 获取详情
            const key = item.dataset.key;
            state.selectedKey = key;
            fetchKeyDetail(key);
        });
    });
}

// 渲染键详情
function renderKeyDetail(info) {
    const container = document.getElementById('key-detail-container');

    let html = `
        <div class="detail-section">
            <div class="detail-label">键名</div>
            <div class="detail-value">${info.key}</div>
        </div>

        <div class="detail-section">
            <div class="detail-label">类型</div>
            <div class="detail-value">
                <span class="badge badge-info">${info.type}</span>
            </div>
        </div>

        <div class="detail-section">
            <div class="detail-label">大小</div>
            <div class="detail-value">${formatSize(info.size)}</div>
        </div>
    `;

    if (info.ttl !== null) {
        html += `
            <div class="detail-section">
                <div class="detail-label">TTL</div>
                <div class="detail-value">
                    <span class="badge ${info.expired ? 'badge-danger' : 'badge-success'}">
                        ${info.ttl.toFixed(2)} 秒
                    </span>
                </div>
            </div>

            <div class="detail-section">
                <div class="detail-label">过期时间</div>
                <div class="detail-value">${info.expires_at}</div>
            </div>
        `;

        if (info.expired) {
            html += `
                <div class="detail-section">
                    <div class="detail-value">
                        <span class="badge badge-danger">已过期</span>
                    </div>
                </div>
            `;
        }
    }

    if (info.value !== null && info.value !== undefined) {
        let displayValue = info.value;

        // 尝试格式化 JSON
        if (typeof info.value === 'object') {
            displayValue = formatJSON(info.value);
        } else if (typeof info.value === 'string' && info.value.startsWith('{')) {
            try {
                displayValue = formatJSON(info.value);
            } catch (e) {
                displayValue = info.value;
            }
        }

        html += `
            <div class="detail-section">
                <div class="detail-label">值</div>
                <div class="detail-value">${displayValue}</div>
            </div>
        `;
    }

    container.innerHTML = html;
}

// 渲染统计信息
function renderStats(stats) {
    const container = document.getElementById('stats-container');

    let html = '';

    // 总览
    html += `
        <div class="stats-card">
            <h3>总览</h3>
            <div class="stats-item">
                <span class="stats-label">总键数</span>
                <span class="stats-value">${stats.total_keys}</span>
            </div>
            <div class="stats-item">
                <span class="stats-label">总大小</span>
                <span class="stats-value">${formatSize(stats.total_size)}</span>
            </div>
        </div>
    `;

    // 类型分布
    if (stats.type_distribution && Object.keys(stats.type_distribution).length > 0) {
        html += '<div class="stats-card"><h3>类型分布</h3>';
        const sorted = Object.entries(stats.type_distribution).sort((a, b) => b[1] - a[1]);
        for (const [type, count] of sorted) {
            const percentage = ((count / stats.total_keys) * 100).toFixed(1);
            html += `
                <div class="stats-item">
                    <span class="stats-label">${type}</span>
                    <span class="stats-value">${count} (${percentage}%)</span>
                </div>
            `;
        }
        html += '</div>';
    }

    // 大小分布
    if (stats.size_distribution) {
        const sizeLabels = {
            tiny: '极小 (< 1KB)',
            small: '小 (1KB - 10KB)',
            medium: '中 (10KB - 100KB)',
            large: '大 (100KB - 1MB)',
            huge: '巨大 (> 1MB)'
        };

        html += '<div class="stats-card"><h3>大小分布</h3>';
        for (const [key, count] of Object.entries(stats.size_distribution)) {
            const percentage = ((count / stats.total_keys) * 100).toFixed(1);
            html += `
                <div class="stats-item">
                    <span class="stats-label">${sizeLabels[key]}</span>
                    <span class="stats-value">${count} (${percentage}%)</span>
                </div>
            `;
        }
        html += '</div>';
    }

    // TTL 状态
    if (stats.ttl_status) {
        html += '<div class="stats-card"><h3>TTL 状态</h3>';
        const ttl = stats.ttl_status;
        const items = [
            { label: '有 TTL', value: ttl.with_ttl },
            { label: '无 TTL', value: ttl.without_ttl },
            { label: '已过期', value: ttl.expired }
        ];

        for (const item of items) {
            const percentage = ((item.value / stats.total_keys) * 100).toFixed(1);
            html += `
                <div class="stats-item">
                    <span class="stats-label">${item.label}</span>
                    <span class="stats-value">${item.value} (${percentage}%)</span>
                </div>
            `;
        }
        html += '</div>';
    }

    container.innerHTML = html;
}

// 更新分页信息
function updatePagination() {
    document.getElementById('total-keys').textContent = state.totalKeys;
    const totalPages = Math.ceil(state.totalKeys / state.pageSize);
    document.getElementById('page-info').textContent =
        `第 ${state.currentPage} / ${totalPages} 页`;

    document.getElementById('prev-page').disabled = state.currentPage <= 1;
    document.getElementById('next-page').disabled = state.currentPage >= totalPages;
}

// ========== 事件处理 ==========

// 刷新键列表
function refreshKeyList() {
    const offset = (state.currentPage - 1) * state.pageSize;
    fetchKeys(state.currentPattern, offset);
}

// 初始化事件监听器
function initEventListeners() {
    // 标签页切换
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tabName = btn.dataset.tab;

            // 更新标签按钮状态
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // 更新标签内容
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.remove('active');
            });
            document.getElementById(`${tabName}-tab`).classList.add('active');

            // 加载对应数据
            if (tabName === 'stats') {
                fetchStats();
            }
        });
    });

    // 搜索
    document.getElementById('search-btn').addEventListener('click', () => {
        const pattern = document.getElementById('search-input').value.trim();
        if (pattern) {
            state.currentPattern = pattern;
            state.currentPage = 1;
            searchKeys(pattern);
        } else {
            state.currentPattern = null;
            state.currentPage = 1;
            refreshKeyList();
        }
    });

    // 回车搜索
    document.getElementById('search-input').addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            document.getElementById('search-btn').click();
        }
    });

    // 刷新
    document.getElementById('refresh-btn').addEventListener('click', () => {
        state.currentPattern = null;
        state.currentPage = 1;
        document.getElementById('search-input').value = '';
        refreshKeyList();
    });

    // 分页
    document.getElementById('prev-page').addEventListener('click', () => {
        if (state.currentPage > 1) {
            state.currentPage--;
            refreshKeyList();
        }
    });

    document.getElementById('next-page').addEventListener('click', () => {
        const totalPages = Math.ceil(state.totalKeys / state.pageSize);
        if (state.currentPage < totalPages) {
            state.currentPage++;
            refreshKeyList();
        }
    });

    // 刷新统计
    document.getElementById('refresh-stats-btn').addEventListener('click', () => {
        fetchStats();
    });

    // 添加/更新键值表单
    document.getElementById('add-key-form').addEventListener('submit', async (e) => {
        e.preventDefault();

        const key = document.getElementById('new-key').value.trim();
        const valueStr = document.getElementById('new-value').value.trim();
        const ttlStr = document.getElementById('new-ttl').value.trim();

        if (!key || !valueStr) {
            showToast('键名和值不能为空', 'error');
            return;
        }

        // 尝试解析为 JSON
        let value;
        try {
            value = JSON.parse(valueStr);
        } catch (e) {
            // 如果不是 JSON，就作为字符串
            value = valueStr;
        }

        const ttl = ttlStr ? parseInt(ttlStr) : null;

        await setKey(key, value, ttl);

        // 清空表单
        document.getElementById('add-key-form').reset();
    });

    // 删除键表单
    document.getElementById('delete-key-form').addEventListener('submit', async (e) => {
        e.preventDefault();

        const key = document.getElementById('delete-key').value.trim();

        if (!key) {
            showToast('键名不能为空', 'error');
            return;
        }

        if (!confirm(`确定要删除键 "${key}" 吗？`)) {
            return;
        }

        await deleteKey(key);

        // 清空表单
        document.getElementById('delete-key-form').reset();
    });
}

// ========== 初始化 ==========

async function init() {
    initEventListeners();
    await fetchDBInfo();
    refreshKeyList();
}

// 页面加载完成后初始化
document.addEventListener('DOMContentLoaded', init);
