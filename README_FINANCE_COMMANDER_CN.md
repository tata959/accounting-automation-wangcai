# R2R Finance Commander（GitHub 部署版）

## 1. 这是什么
`R2R Finance Commander` 是面向财务人员的中文指令助手。你只需要输入：
- `开始月度结账`
- `查看审批状态`
- `审批通过，更新看板`

它会自动调用现有流程脚本，完成数据准备、报表生成和看板更新。

## 2. 目录结构（关键文件）
- `finance_commander.py` 指令入口
- `scripts/01_extract_clean_load.py` 数据抓取与清洗
- `scripts/02_generate_report_n08.py` N08 报表工厂
- `scripts/03_powerbi_refresh_n09.py` N09 看板刷新
- `scripts/04_set_approval_signal.py` 审批信号写入
- `pbi/status.txt` 审批状态文件
- `reports/Financial_Report_YYYY_MM.xlsx` 报表输出
- `pbi/pbi_dataset.csv` 看板数据输出

## 3. 部署到 GitHub（非常具体）

### 步骤 A：在 D 盘准备本地目录
1. 打开 PowerShell。
2. 进入目录：
```powershell
cd D:\R2R_Automation
```

### 步骤 B：初始化仓库并提交
```powershell
git init
git add .
git commit -m "feat: add R2R Finance Commander"
```

### 步骤 C：在 GitHub 创建新仓库
1. 打开 GitHub 网页，新建仓库（建议名：`r2r-finance-commander`）。
2. 不要勾选 README（避免冲突）。

### 步骤 D：关联远程并推送
```powershell
git remote add origin https://github.com/<你的用户名>/r2r-finance-commander.git
git branch -M main
git push -u origin main
```

## 4. 本地运行步骤（财务可直接照做）

### 4.1 安装依赖
```powershell
cd D:\R2R_Automation
pip install -r requirements.txt
```

### 4.2 启动助手（交互模式）
```powershell
python D:\R2R_Automation\finance_commander.py
```

### 4.3 单条指令执行（非交互）
```powershell
python D:\R2R_Automation\finance_commander.py "开始月度结账"
python D:\R2R_Automation\finance_commander.py "查看审批状态"
python D:\R2R_Automation\finance_commander.py "审批通过，更新看板"
```

## 5. 核心逻辑（状态感知 + 引导）

### 5.1 状态感知
助手会同时检查：
1. `pbi/status.txt`
2. 数据库里最新审批状态

判定规则：只要任一处是 `Approved`，即视为“已通过”。

### 5.2 自动分流 N08 / N09
当输入 `开始月度结账`：
1. 先准备账务数据（调用 `01`）。
2. 再看审批状态：
   - `Pending`：执行 N08，生成初步报表，并提示去审批。
   - `Approved`：执行 N09，更新看板并输出摘要。

### 5.3 交互引导
如果是 `Pending`，助手不会报错，而是明确提示：
> 当前为待审批，请审批后输入“审批通过，更新看板”。

### 5.4 一键修复（抓数失败）
如果 `01` 第一次失败，助手会自动再跑一次。
- 重试成功：继续流程
- 重试失败：给出简明错误信息和下一步建议

## 6. 结果推送摘要（中文）
流程完成后，助手会输出类似：
- `流程完成：已处理 5865 条财务记录，最新收入环比 12.00%。`
- 如果波动超过 10%，额外提示：
  - `已检测到收入波动超过 10%，请重点复核。`

## 7. 推荐给财务同事的固定操作顺序
1. `开始月度结账`
2. `查看审批状态`
3. `审批通过，更新看板`

## 8. 可选：配置正式数据权限
当前脚本默认可用 `demo`（稳定支持 IBM）。
若你有正式权限，可设置：
```powershell
$env:ALPHAVANTAGE_API_KEY="你的正式Key"
```
再执行 `开始月度结账`。

## 9. Web 按钮版（新增）

### 9.1 启动方式
1. 双击：`D:\R2R_Automation\start_commander_web.bat`
2. 浏览器访问：`http://127.0.0.1:8787`

### 9.2 页面按钮
- 开始月度结账
- 查看审批状态
- 生成本月初步报表
- 审批通过，更新看板

### 9.3 状态分流
- Pending：生成初步报表并提示审批
- Approved：刷新看板并输出摘要
