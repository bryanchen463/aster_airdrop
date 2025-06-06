from flask import Flask, render_template, request, redirect, url_for, flash
import yaml
import os
import subprocess
import signal
import psutil
import json
import time

app = Flask(__name__)
app.secret_key = 'your-secret-key'  # 用于flash消息

# 存储进程信息的文件
PROCESS_INFO_FILE = 'process_info.json'

def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)

def get_web_config():
    config = load_config()
    web_config = config.get('web', {})
    return {
        'host': web_config.get('host', '0.0.0.0'),
        'port': web_config.get('port', 5000)
    }

def save_config(config):
    with open("config.yaml", "w") as f:
        yaml.dump(config, f, default_flow_style=False)

def save_process_info(pid):
    with open(PROCESS_INFO_FILE, 'w') as f:
        json.dump({'pid': pid, 'start_time': time.time()}, f)

def load_process_info():
    try:
        with open(PROCESS_INFO_FILE, 'r') as f:
            return json.load(f)
    except:
        return {'pid': None, 'start_time': None}

def is_process_running(pid):
    try:
        process = psutil.Process(pid)
        return process.is_running() and process.name().startswith('python')
    except:
        return False

def get_process_status():
    process_info = load_process_info()
    if not process_info.get('pid'):
        return False
    
    if is_process_running(process_info['pid']):
        return True
    
    # 如果进程不存在，清理进程信息文件
    if os.path.exists(PROCESS_INFO_FILE):
        os.remove(PROCESS_INFO_FILE)
    return False

@app.route('/')
def index():
    config = load_config()
    is_running = get_process_status()
    return render_template('index.html', 
                         accounts=config.get('accounts', []),
                         is_running=is_running)

@app.route('/start', methods=['POST'])
def start_process():
    if get_process_status():
        flash('程序已经在运行中！')
        return redirect(url_for('index'))
    
    try:
        # 使用nohup启动main.py，并将输出重定向到日志文件
        with open('nohup.out', 'a') as f:
            process = subprocess.Popen(['nohup', 'python', 'main.py'],
                                    stdout=f,
                                    stderr=f,
                                    preexec_fn=os.setpgrp)  # 创建新的进程组
        
        # 等待一小段时间确保进程启动
        time.sleep(1)
        
        if process.poll() is None:  # 进程仍在运行
            save_process_info(process.pid)
            flash('程序启动成功！')
        else:
            flash('程序启动失败！')
    except Exception as e:
        flash(f'启动失败：{str(e)}')
    return redirect(url_for('index'))

@app.route('/stop', methods=['POST'])
def stop_process():
    process_info = load_process_info()
    if not process_info.get('pid'):
        flash('程序未在运行！')
        return redirect(url_for('index'))
    
    try:
        if is_process_running(process_info['pid']):
            # 获取进程组ID
            process = psutil.Process(process_info['pid'])
            pgid = os.getpgid(process_info['pid'])
            
            # 终止整个进程组
            os.killpg(pgid, signal.SIGTERM)
            
            # 等待进程终止
            try:
                process.wait(timeout=5)
            except psutil.TimeoutExpired:
                # 如果进程没有及时终止，强制结束
                os.killpg(pgid, signal.SIGKILL)
            
            # 清理进程信息文件
            if os.path.exists(PROCESS_INFO_FILE):
                os.remove(PROCESS_INFO_FILE)
            
            flash('程序已停止！')
        else:
            flash('程序未在运行！')
    except Exception as e:
        flash(f'停止失败：{str(e)}')
    return redirect(url_for('index'))

@app.route('/add_account', methods=['POST'])
def add_account():
    config = load_config()
    new_account = {
        'key': request.form['key'],
        'secret': request.form['secret'],
        'proxy': request.form['proxy'],
        'cost_per_day': float(request.form['cost_per_day'])
    }
    
    if 'accounts' not in config:
        config['accounts'] = []
    
    config['accounts'].append(new_account)
    save_config(config)
    flash('账户添加成功！')
    return redirect(url_for('index'))

@app.route('/delete_account/<int:index>')
def delete_account(index):
    config = load_config()
    if 0 <= index < len(config.get('accounts', [])):
        config['accounts'].pop(index)
        save_config(config)
        flash('账户删除成功！')
    return redirect(url_for('index'))

if __name__ == '__main__':
    web_config = get_web_config()
    app.run(
        host=web_config['host'],
        port=web_config['port'],
        debug=False  # 生产环境建议关闭debug模式
    ) 