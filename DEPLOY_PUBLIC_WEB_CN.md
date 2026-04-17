# 公网部署指南（GitHub -> Web）

## 1. 先推送到 GitHub
```powershell
cd D:\R2R_Automation
git init
git add .
git commit -m "feat: 旺财 web 控制台"
git remote add origin https://github.com/<你的账号>/<你的仓库>.git
git branch -M main
git push -u origin main
```

## 2. 在 Render 创建 Web Service
1. 登录 Render。
2. New + -> Web Service。
3. 连接你的 GitHub 仓库。
4. 配置：
   - Runtime: Python
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `python finance_commander_web.py`
5. Environment Variables:
   - `R2R_BASE_DIR=/opt/render/project/src`
6. 点击 Deploy。

## 3. 部署成功后
- Render 会生成一个公网 URL。
- 用户直接打开该 URL，即可在网页按钮中完成：
  - URL 导入
  - CSV/Excel 上传导入
  - 月结、报表、审批、发布流程

## 4. 说明
- GitHub Pages 只适合静态网页，不支持当前这种 Python 后端自动化。
- 因此“GitHub 托管代码 + Render 提供运行网页”是可落地方案。
