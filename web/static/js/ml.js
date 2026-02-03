/**
 * ML Model Training and Prediction Module
 */

// ML API endpoints
const ML_API = {
    train: '/api/ml/train',
    models: '/api/ml/models',
    model: (id) => `/api/ml/models/${id}`,
    predict: '/api/ml/predict',
    modelPredictions: (id) => `/api/ml/models/${id}/predictions`,
    modelPerformance: (id) => `/api/ml/models/${id}/performance`
};

/**
 * Initialize ML module
 */
function initMLModule() {
    console.log('[ML] Initializing module...');

    // Set default dates
    const today = new Date();
    const oneYearAgo = new Date(today);
    oneYearAgo.setFullYear(today.getFullYear() - 1);

    document.getElementById('mlStartDate').value = oneYearAgo.toISOString().split('T')[0];
    document.getElementById('mlEndDate').value = today.toISOString().split('T')[0];

    // Bind events
    bindMLEvents();

    // Load models list
    loadMLModels();

    console.log('[ML] Module initialized');
}

/**
 * Bind ML-related events
 */
function bindMLEvents() {
    // Training form
    document.getElementById('mlTrainForm').addEventListener('submit', handleMLTrain);

    // Refresh models button
    document.getElementById('btnRefreshModels').addEventListener('click', loadMLModels);

    // Prediction form
    document.getElementById('mlPredictForm').addEventListener('submit', handleMLPredict);
}

/**
 * Handle model training
 */
async function handleMLTrain(e) {
    e.preventDefault();

    const formData = {
        symbol: document.getElementById('mlSymbol').value.trim(),
        start_date: document.getElementById('mlStartDate').value,
        end_date: document.getElementById('mlEndDate').value || undefined,
        target_type: document.getElementById('mlTargetType').value,
        model_type: document.getElementById('mlModelType').value,
        add_technical: document.getElementById('mlUseTechnical').checked
    };

    if (!formData.symbol) {
        showError('请输入股票代码');
        return;
    }

    try {
        const response = await fetch(ML_API.train, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });

        const data = await response.json();

        if (data.success) {
            showSuccess(`训练任务已创建！任务ID: ${data.task_id.substring(0, 8)}...`);
            document.getElementById('mlTrainForm').reset();

            // Reset dates
            const today = new Date();
            const oneYearAgo = new Date(today);
            oneYearAgo.setFullYear(today.getFullYear() - 1);
            document.getElementById('mlStartDate').value = oneYearAgo.toISOString().split('T')[0];
            document.getElementById('mlEndDate').value = today.toISOString().split('T')[0];

            // Switch to tasks tab
            setTimeout(() => {
                document.getElementById('tasks-tab').click();
            }, 1000);
        } else {
            showError(data.error || '训练任务创建失败');
        }
    } catch (error) {
        console.error('[ML] Train error:', error);
        showError('训练任务创建失败: ' + error.message);
    }
}

/**
 * Load ML models list
 */
async function loadMLModels() {
    const tbody = document.getElementById('mlModelsTableBody');
    tbody.innerHTML = '<tr><td colspan="5" class="text-center"><div class="spinner-border spinner-border-sm" role="status"></div></td></tr>';

    try {
        const response = await fetch(ML_API.models);
        const data = await response.json();

        if (data.success) {
            if (data.models.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted">暂无训练模型</td></tr>';
                return;
            }

            tbody.innerHTML = '';
            data.models.forEach(model => {
                // Parse metrics if available
                let maeDisplay = '-';
                if (model.metrics) {
                    try {
                        const metrics = typeof model.metrics === 'string' ? JSON.parse(model.metrics) : model.metrics;
                        maeDisplay = metrics.mae ? (metrics.mae * 100).toFixed(4) + '%' : '-';
                    } catch (e) {
                        console.warn('Failed to parse metrics:', e);
                    }
                }

                const row = document.createElement('tr');
                row.innerHTML = `
                    <td><small>${model.model_id.substring(0, 12)}...</small></td>
                    <td>${model.symbol}</td>
                    <td><span class="badge bg-secondary">${model.model_type}</span></td>
                    <td>${maeDisplay}</td>
                    <td>
                        <button class="btn btn-sm btn-outline-primary" onclick="viewModelDetails('${model.model_id}')">
                            <i class="bi bi-info-circle"></i>
                        </button>
                        <button class="btn btn-sm btn-outline-danger" onclick="deleteModel('${model.model_id}')">
                            <i class="bi bi-trash"></i>
                        </button>
                    </td>
                `;
                tbody.appendChild(row);
            });

            // Update model select for prediction
            updateModelSelect(data.models);
        } else {
            tbody.innerHTML = '<tr><td colspan="5" class="text-center text-danger">加载失败</td></tr>';
        }
    } catch (error) {
        console.error('[ML] Load models error:', error);
        tbody.innerHTML = '<tr><td colspan="5" class="text-center text-danger">加载失败</td></tr>';
    }
}

/**
 * Update model select dropdown
 */
function updateModelSelect(models) {
    const select = document.getElementById('mlPredictModel');
    select.innerHTML = '<option value="">-- 选择模型 --</option>';

    models.forEach(model => {
        const option = document.createElement('option');
        option.value = model.model_id;

        // Parse metrics for display
        let maeDisplay = '';
        if (model.metrics) {
            try {
                const metrics = typeof model.metrics === 'string' ? JSON.parse(model.metrics) : model.metrics;
                if (metrics.mae) {
                    maeDisplay = ` | MAE: ${(metrics.mae * 100).toFixed(4)}%`;
                }
            } catch (e) {
                // Ignore parse errors
            }
        }

        option.textContent = `${model.symbol} - ${model.model_type} (${model.created_at.substring(0, 10)})${maeDisplay}`;
        select.appendChild(option);
    });
}

/**
 * Handle prediction
 */
async function handleMLPredict(e) {
    e.preventDefault();

    const formData = {
        model_id: document.getElementById('mlPredictModel').value,
        symbol: document.getElementById('mlPredictSymbol').value.trim()
    };

    if (!formData.model_id) {
        showError('请选择模型');
        return;
    }

    if (!formData.symbol) {
        showError('请输入股票代码');
        return;
    }

    const resultDiv = document.getElementById('mlPredictResult');
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = '<div class="text-center"><div class="spinner-border" role="status"></div></div>';

    try {
        const response = await fetch(ML_API.predict, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });

        const data = await response.json();

        if (data.success) {
            const prediction = data.prediction;
            const target_type = data.target_type || 'return_1d';
            let predictionText, predictionClass;

            if (target_type === 'direction_1d') {
                // 二分类：涨跌方向
                const upProbability = prediction * 100;
                const direction = prediction > 0.5 ? '上涨' : '下跌';
                predictionClass = prediction > 0.5 ? 'text-success' : 'text-danger';
                predictionText = `${direction}概率: ${upProbability.toFixed(2)}%`;
            } else {
                // 回归：收益率
                const direction = prediction > 0 ? '上涨' : '下跌';
                predictionClass = prediction > 0 ? 'text-success' : 'text-danger';
                predictionText = `预测收益率: ${(prediction * 100).toFixed(4)}% (${direction})`;
            }

            resultDiv.innerHTML = `
                <div class="alert alert-info">
                    <h6><i class="bi bi-graph-up"></i> 预测结果</h6>
                    <hr>
                    <p><strong>股票:</strong> ${data.symbol}</p>
                    <p><strong>预测日期:</strong> ${data.date}</p>
                    <p><strong>预测类型:</strong> ${target_type === 'direction_1d' ? '涨跌方向' : '收益率预测'}</p>
                    <p><strong>预测结果:</strong> <span class="${predictionClass}">${predictionText}</span></p>
                    <p><strong>模型:</strong> ${data.model_id.substring(0, 12)}...</p>
                </div>
            `;
        } else {
            resultDiv.innerHTML = `<div class="alert alert-danger">预测失败: ${data.error}</div>`;
        }
    } catch (error) {
        console.error('[ML] Predict error:', error);
        resultDiv.innerHTML = `<div class="alert alert-danger">预测失败: ${error.message}</div>`;
    }
}

/**
 * View model details
 */
async function viewModelDetails(modelId) {
    try {
        const response = await fetch(ML_API.model(modelId));
        const data = await response.json();

        if (data.success) {
            const model = data.model;

            // Show details in a modal or alert
            const details = `
模型ID: ${model.model_id}
股票: ${model.symbol}
类型: ${model.model_type}
创建时间: ${model.created_at}
训练时间: ${model.trained_at}
训练期间: ${model.training_start} ~ ${model.training_end}
样本数: 训练 ${model.train_samples} | 验证 ${model.val_samples} | 测试 ${model.test_samples}
特征数: ${model.n_features}
            `;

            alert(details);
        } else {
            showError('获取模型详情失败');
        }
    } catch (error) {
        console.error('[ML] View model error:', error);
        showError('获取模型详情失败');
    }
}

/**
 * Delete model
 */
async function deleteModel(modelId) {
    if (!confirm('确定要删除此模型吗？')) {
        return;
    }

    try {
        const response = await fetch(ML_API.model(modelId), {
            method: 'DELETE'
        });

        const data = await response.json();

        if (data.success) {
            showSuccess('模型已删除');
            loadMLModels();
        } else {
            showError(data.error || '删除失败');
        }
    } catch (error) {
        console.error('[ML] Delete model error:', error);
        showError('删除失败');
    }
}

/**
 * Show error message
 */
function showError(message) {
    // Simple alert for now - could be enhanced with toast notifications
    alert('错误: ' + message);
}

/**
 * Show success message
 */
function showSuccess(message) {
    // Simple alert for now - could be enhanced with toast notifications
    alert(message);
}

// Auto-initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initMLModule);
} else {
    initMLModule();
}
