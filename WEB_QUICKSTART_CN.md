# Web 版本快速启动

1. 双击 `D:\R2R_Automation\start_commander_web.bat`。
2. 浏览器打开 `http://127.0.0.1:8787`。
3. 页面按钮直接操作：
   - 开始月度结账
   - 查看审批状态
   - 生成本月初步报表
   - 审批通过，更新看板

## GitHub 发布步骤
```powershell
cd D:\R2R_Automation
git init
git add .
git commit -m "feat: web-based R2R finance commander"
git remote add origin https://github.com/<your-account>/<repo>.git
git branch -M main
git push -u origin main
```

## 关键文件
- `D:\R2R_Automation\finance_commander_web.py`
- `D:\R2R_Automation\finance_commander.py`
- `D:\R2R_Automation\scripts\01_extract_clean_load.py`
- `D:\R2R_Automation\scripts\02_generate_report_n08.py`
- `D:\R2R_Automation\scripts\03_powerbi_refresh_n09.py`
- `D:\R2R_Automation\scripts\04_set_approval_signal.py`
- `D:\R2R_Automation\PRD_R2R_Finance_Commander_Web_CN.md`
