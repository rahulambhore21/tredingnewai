# Deployment Guide: Trading Bot & Dashboard

This guide provides step-by-step instructions on how to deploy and run the AI Trading Bot and its observability dashboard.

---

## Architecture & Requirements

Since the bot uses the official `MetaTrader5` Python library to place trades, **it must run on a Windows environment** (Windows 10/11 or Windows Server 2019/2022) with the MetaTrader 5 desktop terminal installed. The library communicates with the MT5 terminal via inter-process communication (IPC) on the same machine.

### Key Components:
1. **MetaTrader 5 Terminal**: Must be open, running, and logged into your broker account.
2. **Python Bot (`main.py`)**: Multi-threaded core agent pipeline executing trades, watching prices, and performing GPT-4o analysis.
3. **Observability Dashboard (`dashboard/app.py`)**: Flask-based read-only web server running on port `5001` to monitor logs, database states, and system health.

---

## Choose Your Deployment Model

### Option A: Cloud Windows VPS (Recommended for Production)
To keep the bot running 24/7 without keeping your personal computer turned on, deploy it on a Windows Server VPS.

1. **Rent a Windows Server VPS**: 
   * Choose a provider like **Vultr**, **AWS (EC2)**, **DigitalOcean**, or a specialized Forex VPS (e.g., **ForexVPS**, **Beeks**).
   * **Location**: Choose a VPS location closest to your broker's server (e.g., London or New York) to minimize execution latency.
   * **Specs**: Minimum 2 Cores, 4GB RAM (Windows requires at least 4GB to run smoothly with MT5 and Python).

2. **Set up the Windows Server VPS**:
   * Connect via Remote Desktop (RDP).
   * Download and install the **MetaTrader 5 terminal** from your broker.
   * Log into your broker trading account, check the "Keep Personal Settings" option, and ensure you can see live charts.
   * Download and install **Python 3.10 or 3.11** (Ensure you check **"Add Python to PATH"** during installation).
   * Install **Git** (optional, to clone and update the codebase).

3. **Deploy the Codebase**:
   * Clone/copy the project files to the VPS (e.g., to `C:\TradingBot`).
   * Open Command Prompt or PowerShell, navigate to `C:\TradingBot`, and install dependencies:
     ```cmd
     pip install -r requirements.txt
     pip install -r dashboard/requirements.txt
     ```

4. **Configure Environment Variables**:
   * Create a `.env` file from the template:
     ```cmd
     copy .env.example .env
     ```
   * Open `.env` and fill in your details:
     * `MT5_LOGIN`: Your account number.
     * `MT5_PASSWORD`: Your trading password.
     * `MT5_SERVER`: Your broker's server name (found in MT5 under Account Info).
     * `OPENAI_API_KEY`: Your OpenAI API key for analysis.
     * `EXECUTION_LIVE`: Set to `True` for live trading, or `False` for dry-run/paper trading.

---

### Option B: Local Deployment (Windows PC)
Ideal for testing or if you keep your PC running during trading hours.

1. Install Python 3.10+ and ensure the MT5 terminal is open and logged in.
2. Open PowerShell/CMD in the project directory.
3. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   pip install -r dashboard/requirements.txt
   ```
4. Create and fill in your `.env` file.

---

## Running the Bot & Dashboard

For the bot to function, **both** the MT5 Desktop Terminal and the Python processes must run.

### Step 1: Start MetaTrader 5
Ensure the MT5 terminal is open and actively running on your desktop.

### Step 2: Run the Trading Bot
Open a command prompt/PowerShell terminal and run:
```cmd
python main.py
```
*You will see the bot validate the configuration, connect to MT5, run the initial Support & Resistance scan, and begin the tick-watching loop.*

### Step 3: Run the Dashboard
Open a **second** terminal and run:
```cmd
python dashboard/app.py
```
*The dashboard will start on `http://localhost:5001` (or `http://0.0.0.0:5001`).*

---

## Production Reliability & Automation

If you are running on a VPS, you need the bot to restart automatically if the server reboots or if the processes crash.

### Method 1: Use PM2 (Recommended)
PM2 is a production process manager that works great on Windows to keep Python scripts alive.

1. Install **Node.js** on the VPS.
2. Install PM2 globally:
   ```cmd
   npm install pm2 -g
   npm install pm2-windows-startup -g
   pm2-startup install
   ```
3. Start the bot and the dashboard with PM2:
   ```cmd
   pm2 start main.py --name "trading-bot" --interpreter python
   pm2 start dashboard/app.py --name "trading-dashboard" --interpreter python
   ```
4. Save the PM2 process list so it recovers on reboot:
   ```cmd
   pm2 save
   ```

### Method 2: NSSM (Non-Sucking Service Manager)
You can register both scripts as native Windows Services:

1. Download **NSSM** (nssm.cc) and add it to your PATH.
2. Run `nssm install TradingBot` in CMD.
3. Configure the service:
   * **Path**: `C:\Users\<username>\AppData\Local\Programs\Python\Python310\python.exe` (Path to python.exe)
   * **Startup directory**: `C:\TradingBot`
   * **Arguments**: `main.py`
4. Click *Install service*.
5. Repeat for the dashboard (`dashboard/app.py`).

---

## Accessing the Dashboard Remotely (VPS only)

Since the dashboard binds to `0.0.0.0:5001`, you can access it over the internet.

### 1. Allow the Port in Windows Firewall
On your Windows VPS, open PowerShell as **Administrator** and run this command to allow external traffic on port `5001`:
```powershell
New-NetFirewallRule -DisplayName "Trading Bot Dashboard" -Direction Inbound -LocalPort 5001 -Protocol TCP -Action Allow
```
Now, you can open your browser and navigate to `http://<YOUR_VPS_IP>:5001`.

### 2. Secure Access (Ngrok or Cloudflare Tunnels)
For security, it is highly recommended **not** to expose port 5001 directly to the public internet. Instead, use a secure tunnel:

* **Ngrok Setup**:
  1. Download Ngrok on the VPS.
  2. Authenticate and run:
     ```cmd
     ngrok http 5001
     ```
  3. Use the secure `https://...ngrok-free.app` URL provided to monitor your bot from your phone or local PC.
