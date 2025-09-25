import redis
import logging
import threading
from threading import Lock
import queue

from flask import Flask, render_template, render_template_string, request, jsonify, Response, stream_with_context 
import time
import os
import json


LOG_FILE = "redis_pubsub.log"
redishost= os.getenv("REDIS_HOST","localhost") #Permits setting a different redishost using env var - default is "localhost"
print(f"Redis host set to: {redishost}")

app = Flask(__name__)
client = redis.StrictRedis(redishost, port=6379, decode_responses=True)
redis_client = client

lock = Lock()

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Active servers
def get_active_servers():
    """Retrieve the list of active servers from Redis."""
    return redis_client.hgetall("ACTION_SERVER_ACTIVE") or {}

def event_stream():
    """Stream live server status updates."""
    pubsub = redis_client.pubsub()
    pubsub.psubscribe("ACTION_SERVER_PUBSUB:*")

    for message in pubsub.listen():
        if message["type"] == "pmessage":
            server_name = message["channel"].split(":")[-1]
            status = redis_client.hget("ACTION_SERVER_ACTIVE", server_name)
            status = time.strftime('%Y-%m-%d %H:%M:%S')
            server_id = redis_client.hget("<server id>", server_name)
            
            # print(f"data: {server_name}:{status}:{server_id}")
            yield f"data: {json.dumps({server_name: status})}\n\n"
        


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# WebLog

def tail(file, n=20):
    """Read last n lines of a file efficiently."""
    with open(file, 'rb') as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        lines = []
        line = ''
        pos = end

        while pos > 0 and len(lines) < n:
            pos -= 1
            f.seek(pos)
            char = f.read(1)
            if char == b'\n':
                if line:
                    lines.append(line[::-1])
                line = ''
            else:
                line += char.decode(errors='ignore')
        if line:
            lines.append(line[::-1])
        return '\n'.join(reversed(lines))

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

def get_last_lines(filename, num_lines=20):
    """Read the last num_lines from a file."""
    if not os.path.exists(filename):
        return []
    with open(filename, "r") as f:
        lines = f.readlines()
    return lines[-num_lines:]


def setup_logger():
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.WARNING,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
 
def message_handler(message):
    setup_logger()
    
    if message['type'] == 'message':
        if 'HEARTBEAT' in message['data']:
            return  # Skip logging this message
        logging.error(f"{message['channel']} | {message['data']}")

def log_all_channels():
    setup_logger()
    client = redis.StrictRedis(redishost, port=6379, decode_responses=True)
    pubsub = client.pubsub()
    
    channels = client.pubsub_channels()
    if not channels:
        logging.error("No active channels found.")
        return
    
    pubsub.subscribe(channels)
    
    # logging.error("Subscribed to channels: " + ", ".join(channels))
    # print("Subscribed to channels: " + ", ".join(channels))
    for channel in channels:
        print("Subscribed to channel: " + channel) 
        logging.error("Subscribed to channel: " + channel) 

    
    for message in pubsub.listen():
        # print(message)
        message_handler(message)
        #logging.error(message)

def server_list():
    setup_logger()
    
    client = redis.StrictRedis(redishost, port=6379, decode_responses=True)
    pubsub = client.pubsub()
    
    channels = client.pubsub_channels()
    if not channels:
        logging.error("No active channels found.")
        return
    # print(channels)

    return channels

@app.route("/showlastlog")
def ShowNodeLog():
    try:
        with open("show.last.log", "r", encoding="utf-8") as f:
            message = f.read().strip().replace("\n","\\n")
    except FileNotFoundError:
        message = "⚠️ No log file found."
    except Exception as e:
        message = f"Error reading log: {e}"

    return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Main Page</title>
            <script>
                function showWarning() {
                    // open new window 800x600
                    let logWindow = window.open("", "LogWindow", "width=800,height=600,scrollbars=yes,resizable=yes");
                    logWindow.document.write("<pre>{{ message }}</pre>");
                }
            </script>
        </head>
        <body>
            <h1>Main Page</h1>
            <button onclick="showWarning()">Show Log Message</button>
        </body>
        </html>
    """, message=message)




@app.route("/", methods=["GET", "POST"])
def index():
    setup_logger()
    
    if request.method == "POST":
        message = request.form.get("message", "").strip()
        server = request.form.get("server", "").strip()
        command = request.form.get("command", "").strip()
        
        if message:
            #add_to_log(LOG_FILE, message)
            logging.error(message)
        if server:
            if command:
                logging.error("Server: " + server + " Action: " + command)
                cmd =  "python dispatcher_command.py " + server + " " + command + " &"
                logging.error(cmd)
                os.system(cmd)
    
    # num_lines = 20
    # log_lines ={} 
    # log_lines = get_last_lines(LOG_FILE, num_lines)

    # servers = server_list()
    servers = get_active_servers()
    print(servers)

    return render_template_string(TEMPLATE, servers=servers)

TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <title>Redis Dispatcher WebMonitor</title>
    <style>
        body {
            font-family: sans-serif;
        }
        .tabs {
            display: flex;
            margin-bottom: 10px;
        }
        .tab-button {
            padding: 10px 20px;
            cursor: pointer;
            background: #eee;
            border: 1px solid #ccc;
            border-bottom: none;
            margin-right: 5px;
        }
        .tab-button.active {
            background: #fff;
            font-weight: bold;
        }
        .tab-content {
            display: none;
            border: 1px solid #ccc;
            padding: 15px;
        }
        .tab-content.active {
            display: block;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 3px;
            text-align: left;
        }
        .on {
            background-color: #d4edda;
            color: #155724;
        }
        .off {
            background-color: #f8d7da;
            color: #721c24;
        }
        .blue {
            background-color: #add8e6;
            color: #002366;
        }
        .green {
            background-color: #d4edda;
            color: #155724;
        }
        .red {
            background-color: #f8d7da; /* Light Red */
            color: #721c24;            /* Dark Red */
        }
        .orange {
            background-color: #fff3cd; /* Light Orange/Yellow */
            color: #856404;            /* Dark Orange/Brown */
        }
	.grey {
            background-color: #CCCCCC; /* Light Grey */
            color: #ffffff;            /* Dark Grey */
        }
        
        button {
            margin-right: 5px;
        }
    </style>
</head>
<body>

<div class="tabs">
    <div class="tab-button active" onclick="showTab('monitor')">Server Monitor</div>
    <div class="tab-button" onclick="showTab('executing')">Actions Monitor</div>
<!--    <div class="tab-button" onclick="showTab('dispatcher')">Dispatcher Command</div> -->
<!--    <div class="tab-button" onclick="showTab('addlog')">Add to Log</div> -->
    <div class="tab-button" onclick="showTab('logviewer')">Log Viewer</div>
    <div class="tab-button" onclick="showTab('heartbeat')">Server HeartBeat</div>
    
</div>

<div id="monitor" class="tab-content active">
    <h3>Redis Server Status Monitor (Live)</h3>
    <table border="1">
        <thead>
            <tr><th>Server Key</th><th>Status</th><th>Controls</th></tr>
        </thead>
        <tbody id="status-table"></tbody>
    </table>
</div>

<div id="dispatcher" class="tab-content">
    <h3>Dispatcher Command</h3>
    <form id="commandForm">
        <input type="text" name="server" placeholder="localhost" required />
        <input type="text" name="command" placeholder="command" required style="width: 600px;" />
        <button type="submit">Send</button>
    </form>
</div>

<!--
<div id="addlog" class="tab-content">
    <h3>Add to Log - Debugging</h3>
    <form id="logForm">
        <input type="text" name="message" required style="width: 700px;" />
        <button type="submit">Add to Log</button>
    </form>
</div>
-->

<div id="logviewer" class="tab-content">
    <h3>Add to Log - Debugging</h3>
    <form id="logForm">
        <input type="text" name="message" required style="width: 700px;" />
        <button type="submit">Add to Log</button>
    </form>
    <h3>Live Log Viewer</h3>
    <pre id="log" style="background:#CCC; color:#666; padding:1em; max-height:90vh; overflow:auto; font-size:1rem; font-family:'Roboto Mono','Fira Code','Source Code Pro',monospace;"></pre>
</div>

<div id="heartbeat" class="tab-content">
    <h3>Server Status [LAST HEARTBEAT]</h3>
    <ul id="server-list"></ul>
</div>

<div id="executing" class="tab-content active">
    <h3>Actions</h3>
    <form id="unifiedForm">
    <input type="text" name="server" placeholder="localhost" style="width: 150px;" required />
    <input type="text" name="tree" placeholder="tree" style="width: 150px;" required />
    <input type="text" name="shot" placeholder="shot_number" style="width: 150px;" required />
    <button type="submit" name="actionType" value="create">Create Pulse</button>
    <button type="submit" name="actionType" value="build">Build Table</button>

    <input type="text" name="action" placeholder="action" style="width: 150px;" />
    <button type="submit" name="actionType" value="dispatch">Dispatch</button>

    <input type="text" name="command" placeholder="command" style="width: 300px;" />
    <button type="submit" name="actionType" value="send">Send</button>
    </form>

    <h3>Redis Action Status Monitor</h3>
    <h2 id="action-summary"></h2>

    <h3>Redis Action Status Monitor - Current Phase </h3>   
   
    
    <table border="1">
        <thead>
            <tr><th>Tree</th><th>Shot</th><th>Server</th><th>Node</th><th>State</th><th>Status</th><th>Phase</th><th>Controls</th></tr>
        </thead>
        <tbody id="actions-table-active"></tbody>

    </table>

    <h3>Redis Action Status Monitor -All Phases </h3>   

    <table border="1">
        <thead>
            <tr><th>Tree</th><th>Shot</th><th>Server</th><th>Node</th><th>State</th><th>Status</th><th>Phase</th><th>Controls</th></tr>
        </thead>
        <tbody id="actions-table"></tbody>
    </table>
</div>

<div id="clock" style="font-size: 1.5em; font-family: monospace; margin-top: 20px;"></div>

    

<script>
    function showTab(tabId) {
        document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
        document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
        document.getElementById(tabId).classList.add('active');
        event.target.classList.add('active');
    }

    const eventSource = new EventSource("/time");
    eventSource.onmessage = function(event) {
        document.getElementById("clock").textContent = event.data;
    };

    const eventSourceServers = new EventSource('/server_active');
    eventSourceServers.onmessage = function(event) {
        const data = JSON.parse(event.data);
        const tbody = document.getElementById('status-table');
        tbody.innerHTML = '';
        data.forEach(item => {
            const row = document.createElement('tr');
            row.className = item.status === 'ON' ? 'on' : 'off';
            row.innerHTML = `
                <td>${item.key}</td>
                <td>${item.status}</td>
                <td>
                    <button onclick="sendCommand('${item.key}', 'START')">Start Server</button>
                    <button onclick="sendCommand('${item.key}', 'QUIT')">Quit Server</button>
                    <button onclick="sendCommand('${item.key}', 'RESTART')">Restart Server</button>
                    <button onclick="sendCommand('${item.key}', 'KILL')">Kill Server</button>
<!--
                    <button onclick="sendCommand('${item.key}', 'STOP')">Stop Actions</button>
                    <button onclick="sendCommand('${item.key}', 'SERVER_LOG')">Server Log</button>
-->
                </td>`;
            tbody.appendChild(row);
        });
    };

        
    const eventSourceActions = new EventSource('/action_status');
    eventSourceActions.onmessage = function(event) {
        const parsed = JSON.parse(event.data);
        const data = parsed.data;
        const data_active = parsed.data_active;

        //const data = JSON.parse(event.data);
        //const data_active = JSON.parse(event.data_active);

        const tbody = document.getElementById('actions-table');
        const tbody_active = document.getElementById('actions-table-active');

        const summary = document.getElementById('action-summary');
        tbody.innerHTML = '';
        tbody_active.innerHTML = '';


        // Contadores de status
        let total = 0;
        let dispatched = 0;
        let not_dispatched = 0;
        let doing = 0;
        let done = 0;
        let failed = 0;
        let aborted = 0; 
        let success = 0; 
        let unknown = 0; 
        let off = 0;     


        data_active.forEach(item => {
            const row_active = document.createElement('tr');

            if (item.status === 'NOT_DISPATCHED') {
                row_active.className = 'blue';
            } else if (item.status === 'DONE' && item.state === 'Success') {
                row_active.className = 'green';
            } else if (item.status === 'DONE' && item.state === '0') {
                row_active.className = 'red';
                item.state = 'Failed';
            } else if (item.status === 'DONE' && item.state === 'Failure') {
                row_active.className = 'red';
            } else if (item.status === 'DONE' && item.state === 'Aborted') {
                row_active.className = 'red';
            } else if (item.status === 'Unknown') {
                row_active.className = 'orange';
            } else if (item.status === 'OFF') {
                row_active.className = 'grey';           
            } else if (item.status === 'SERVER_OFF') {
                row_active.className = 'grey';           
            } else {
                row_active.className = 'orange';
            }
        
            row_active.innerHTML = `
                <td>${item.tree}</td>
                <td>${item.shot}</td>
                <td>${item.server}</td>
                <td>${item.key}</td>
                <td>${item.status}</td>
                <td>${item.state}</td>
                <td>${item.phase}</td>

                <td>
                   <button onclick="sendActionCommand('${item.tree}' , '${item.shot}' , '${item.server}',  '${item.key}', 'DISPATCH')">Dispatch</button>
                   <button onclick="sendActionCommand('${item.tree}' , '${item.shot}' , '${item.server}',  '${item.key}', 'ABORT')">Abort</button>
                   <button onclick="sendActionCommand('${item.tree}' , '${item.shot}' , '${item.server}',  '${item.key}', 'LOGS')">Logs</button>
                </td>`;
            tbody_active.appendChild(row_active);
        });


        data.forEach(item => {
            total++;

            const row = document.createElement('tr');

            if (item.status === 'DONE'){
                done++;
            }  

            if (item.status === 'NOT_DISPATCHED') {
                row.className = 'blue';
                not_dispatched++;

            } else if (item.status === 'DONE' && item.state === 'Success') {
                row.className = 'green';
                success++;
            } else if (item.status === 'DONE' && item.state === '0') {
                row.className = 'red';
                item.state = 'Failed';
                failed++;
            } else if (item.status === 'DONE' && item.state === 'Failure') {
                row.className = 'red';
                failed++;
            } else if (item.status === 'DONE' && item.state === 'Aborted') {
                row.className = 'red';
                aborted++;

            } else if (item.state === 'Unknown') {
                row.className = 'orange';
            } else if (item.status === 'OFF') {
                row.className = 'grey';
                off++;
            } else if (item.status === 'SERVER_OFF') {
                row.className = 'grey';
                off++;
            
            } else {
                row.className = 'orange';
            }
            
            dispatched=total-not_dispatched-off;
            doing=total-not_dispatched-done-off;

            unknown=done-failed-success-aborted;
            
            row.innerHTML = `
                <td>${item.tree}</td>
                <td>${item.shot}</td>
                <td>${item.server}</td>
                <td>${item.key}</td>
                <td>${item.status}</td>
                <td>${item.state}</td>
                <td>${item.phase}</td>

                <td>
                   <button onclick="sendActionCommand('${item.tree}' , '${item.shot}' , '${item.server}',  '${item.key}', 'DISPATCH')">Dispatch</button>
                   <button onclick="sendActionCommand('${item.tree}' , '${item.shot}' , '${item.server}',  '${item.key}', 'ABORT')">Abort</button>
                   <button onclick="sendActionCommand('${item.tree}' , '${item.shot}' , '${item.server}',  '${item.key}', 'LOGS')">Logs</button>
                </td>`;
            tbody.appendChild(row);
        });
        summary.innerHTML = `
            <font color="brown">
                Active actions: ${total-off} &nbsp;
            </font>     
            <font color="blue">
                Dispatched: ${dispatched} &nbsp;
                Doing: ${doing} &nbsp;
                Done: ${done} &nbsp;
            </font>     
            
            <font color="green"> Success: ${success} &nbsp;  </font> 
            <font color="red"> Failed: ${failed} &nbsp;  
            <font color="red"> Aborted: ${aborted} &nbsp;  </font> 
            <font color="orange"> Unknown: ${unknown}  </font> 
        `;      


    };


    function sendCommand(serverKey, command) {
        fetch('/server_command', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ server_key: serverKey, command: command })
        })
        .then(response => response.json())
        .then(data => console.log(data.message))
        .catch(error => console.error('Error sending command:', error));
    }  


    function sendActionCommand(tree, shot, server, key, command) {
        fetch('/server_actioncommand', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tree: tree, shot: shot, server: server, key: key, command: command })
        })
        .then(response => response.json())
        .then(data => console.log(data.message))
        .catch(error => console.error('Error sending command:', error));
    }

    document.getElementById("commandForm").addEventListener("submit", function(e) {
        e.preventDefault();
        const formData = new FormData(this);
        fetch('/post', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(Object.fromEntries(formData.entries()))
        }).then(res => {
            <!-- alert(res.ok ? "Command sent successfully!" : "Failed to send command."); -->
        }).catch(err => alert("Error: " + err));
    });

    document.getElementById("unifiedForm").addEventListener("submit", function(e) {
        e.preventDefault();

        const clickedButton = document.activeElement;
        const formData = new FormData(this);
        formData.append("actionType", clickedButton.value);

        fetch('/post', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(Object.fromEntries(formData.entries()))
        }).then(res => {
           <!-- alert(res.ok ? "Command sent successfully!" : "Failed to send command."); -->
        }).catch(err => alert("Error: " + err));
    });
    
    document.getElementById("logForm").addEventListener("submit", function(e) {
        e.preventDefault();
        const formData = new FormData(this);
        fetch('/add_to_log', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(Object.fromEntries(formData.entries()))
        }).then(res => {
            alert(res.ok ? "Log message added successfully!" : "Failed to add message to log.");
        }).catch(err => alert("Error: " + err));
    });

    const log = document.getElementById("log");
    const es = new EventSource("/stream");
    es.onmessage = function(event) {
        log.textContent = event.data;
        log.scrollTop = log.scrollHeight;
    };

    const esHeartbeat = new EventSource("/events");
    esHeartbeat.onmessage = function(event) {
        const data = JSON.parse(event.data);
        for (const server in data) {
            const status = data[server];
            let el = document.getElementById(server);
            if (!el) {
                el = document.createElement("li");
                el.id = server;
                document.getElementById("server-list").appendChild(el);
            }
            el.textContent = `${server}: ${status}`;
        }
    };
</script>

</body>
</html>

"""

@app.route('/time')
def time_stream():
    def generate_time():
        while True:
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            yield f"data: {now}\n\n"
            time.sleep(1)
    return Response(generate_time(), mimetype='text/event-stream')

@app.route('/server_active')
def get_data():
    def generate_data():
        while True:
            server_keys = redis_client.keys("ACTION_SERVER_ACTIVE*")
            data = []

            for key in server_keys:
                value = redis_client.hget(key, '1')
                key_parts=key.split(":")
                key=key_parts[1] 

                data.append({
                    'key': key,
                    'status': value
                })

            # Sort: first by whether status is 'ON' (False < True), then alphabetically by key
            data.sort(key=lambda x: (x['status'] != 'ON', x['key']))

            yield f"data: {json.dumps(data)}\n\n"
            #print(f"data: {json.dumps(data)}\n\n")
            time.sleep(5)
    return Response(generate_data(), mimetype='text/event-stream')

@app.route('/action_status')
def get_action_data():
    def generate_action_data():
        while True:
            server_keys = redis_client.keys("CURRENT_PHASE*")
            data_4 =[] 
            for key in server_keys:
                key_parts=key.split(":")
                value = redis_client.hgetall(key)
                #print(f"{value}")
                for v1, v2 in value.items():
                    data_4.append({
                        'tree': v1,
                        'phase': v2
                    })
            # print(f"{data_4}")

                
            server_keys = redis_client.keys("ACTION_PHASE*")
            data_3 =[] 
            for key in server_keys:
                key_parts=key.split(":")
                value = redis_client.hgetall(key)
                #print(f"{value}")
                for v1, v2 in value.items():
                    data_3.append({
                        'tree' :  key_parts[1],
                        'shot' :  key_parts[2],
                        'key': v1,
                        'phase': v2
                    })


            server_keys = redis_client.keys("ACTION_STATUS*")
            data_2 =[] 
            for key in server_keys:
                key_parts=key.split(":")
                value = redis_client.hgetall(key)
                #print(f"{value}")
                for v1, v2 in value.items():
                    data_2.append({
                        'tree' :  key_parts[1],
                        'shot' :  key_parts[2],
                        'key': v1,
                        'state': v2
                    })


            server_keys = redis_client.keys("ACTION_INFO*")
            data = []
            for key in server_keys:
                key_parts=key.split(":")
                value = redis_client.hgetall(key)
                #print(f"{value}")
                for v1, v2 in value.items():
                    data.append({
                        'tree' :  key_parts[1],
                        'shot' :  key_parts[2],
                        'server': key_parts[-1],
                        'key': v1,
                        'status': v2
                    })
            
            # Step 1: Create a lookup dictionary from data_2
            state_lookup = {
                (item['tree'], item['shot'], item['key']): item['state']
                for item in data_2
            }

            # Step 2: Add 'state' to each item in data using the lookup
            for item in data:
                key = (item['tree'], item['shot'], item['key'])
                item['state'] = state_lookup.get(key, 'none')

            # Step 1: Create a lookup dictionary from data_3
            state_lookup = {
                (item['tree'], item['shot'], item['key']): item['phase']
                for item in data_3
            }

            # Step 2: Add 'phase' to each item in data using the lookup
            for item in data:
                key = (item['tree'], item['shot'], item['key'])
                item['phase'] = state_lookup.get(key, 'none')


            # print(data_active)
            # print(data)

            # STEP 3: Fetch last Build Table info
            last_build = redis_client.get("LAST_BUILD_TABLE")
            if last_build:
                try:
                    last_build = json.loads(last_build)
                    last_tree = last_build.get("tree")
                    last_shot = last_build.get("shot")
                except json.JSONDecodeError:
                    last_tree = last_shot = None
            else:
                last_tree = last_shot = None

            # Filter if last_tree and last_shot are available
            if last_tree and last_shot:
                filtered_data = [
                    item for item in data
                    if item['tree'] == last_tree and item['shot'] == last_shot
                ]
            else:
                filtered_data = data 

            # Step 4: Generate data_active by matching tree and phase with data_4
            phase_lookup_set = {(item['tree'], item['phase']) for item in data_4}

            data_active = [
                item for item in filtered_data
                if (item['tree'], item['phase']) in phase_lookup_set
            ]                           

            sorted_data = sorted(
                filtered_data,
                key=lambda x: (-int(x['shot']), -ord(x['server'][0] ), x['key'])
            )
            # Sort: first by whether status is 'ON' (False < True), then alphabetically by key
            sorted_data.sort(key=lambda x: (x['status'] == 'OFF', x['key']))

            #yield f"data_active: {json.dumps(data_active)}\n\n"
            #yield f"data: {json.dumps(sorted_data)}\n\n"
            yield f"data: {json.dumps({'data': sorted_data, 'data_active': data_active})}\n\n"

            #print(f"data: {json.dumps(data)}\n\n")
            time.sleep(1)
    return Response(generate_action_data(), mimetype='text/event-stream')

@app.route('/server_command', methods=['POST'])
def handle_command():
    setup_logger()
    data = request.json
    server_key = data.get('server_key')
    command = data.get('command')

    # Example: Print or log the received command
    # print(f"Received command: {server_key} {command}")
    # logging.warning(f"Received command: {server_key} {command}")

    if command == 'START':
        cmd = f"server_command.sh {server_key} {command} &"
        os.system(cmd)

    if command == 'STOP' :
        cmd = f"python dispatcher_command.py {redishost} server_stop {server_key} 1 &"
        os.system(cmd)
    if command == 'RESTART':
        cmd = f"python dispatcher_command.py {redishost} server_restart {server_key} 1 &"
        os.system(cmd)

    if command == 'QUIT':
        cmd = f"python dispatcher_command.py {redishost} server_quit {server_key} 1 &"
        os.system(cmd)

    if command == 'KILL':
        cmd = f"server_command.sh {server_key} {command} &"
        os.system(cmd)

    print(f"Running shell command: {cmd} ")
    logging.warning(f"Running shell command: {cmd} ")



    # TODO: Add real logic to handle Redis command dispatch
    # return {'message': f'Command "{command}" sent to "{server_key}"'}, 200


    return '', 200

@app.route('/server_actioncommand', methods=['POST'])
def handle_actioncommand():
    setup_logger()

    data = request.json
    tree = data.get('tree')
    shot = data.get('shot')
    server = data.get('server')
    key = data.get('key')
    command = data.get('command')

    
    if command == "ABORT":
        print(f"publish: ACTION_SERVER_PUBSUB:{server} , ABORT+\{key}")
        client.publish(f"ACTION_SERVER_PUBSUB:{server}", f"ABORT+\{key}")

    if command == "DISPATCH":
        print(f"lpush: ACTION_SERVER_TODO:{server} , {tree}+{shot}+\{key}+0+0+0 ")
        client.lpush(f"ACTION_SERVER_TODO:{server}", f"{tree}+{shot}+\{key}+0+0+0")
        
        print(f"publish: ACTION_SERVER_PUBSUB:{server} , DO ")
        client.publish(f"ACTION_SERVER_PUBSUB:{server}", "DO")

    if command == "LOGS":
        #print(f"TO BE IMPLEMENTED: {server} {key} {command}")
        cmd1 = f"echo SHOW LOG: {tree} {shot} "+"\\"+"\\"+f"{key} {redishost} > show.last.log"
        cmd2 = f"python show_log.py {tree} {shot} "+"\\"+"\\"+f"{key} {redishost} >> show.last.log"
        os.system(cmd1)
        os.system(cmd2)
        ShowNodeLog()


    # Example: Print or log the received command
    print(f"Received command:{server} {key} {command}")

    logging.warning(f"Received command: {key} {command}")

    # TODO: Add real logic to handle Redis command dispatch
    # return {'message': f'Command "{command}" sent to "{server_key}"'}, 200
    return '', 200


@app.route('/post', methods=['POST'])
def handle_post():
    data = request.get_json()
    server = data.get('server', '').strip()
    tree = data.get('tree', '').strip()
    shot = data.get('shot', '').strip()
    action = data.get('action', '').strip()
    command = data.get('command', '').strip()
    action_type = data.get('actionType', '').strip()

    print(f"Action Type: {action_type}")
    print(f"Server: {server}, Tree: {tree}, Shot: {shot}, Action: {action}, Command: {command}")

    client.set("LAST_BUILD_TABLE", json.dumps({'tree': tree, 'shot': shot}))


    setup_logger()
    if action_type == 'send' and command:
        cmd = f"python dispatcher_command.py {server} {command}"
    elif action_type == 'dispatch':
        cmd = f"python dispatcher_command.py {server} do_phase {tree} {shot} {action}"
    elif action_type == 'create':
        cmd = f"python dispatcher_command.py {server} create_pulse {tree} {shot}"
    elif action_type == 'build':
        cmd = f"python dispatcher_command.py {server} build_tables {tree} {shot}"
    else:
        logging.error("Invalid or missing parameters.")
        return '', 400

    cmd= cmd + " &"
    logging.error(cmd)
    os.system(cmd)
    return '', 200

@app.route('/add_to_log', methods=['POST'])
def add_to_log():
    setup_logger()
    data = request.get_json()
    message = data.get('message')
    print(f"Log message received: {message}")
    
    #add_to_log(LOG_FILE, message)
    logging.error(message)    
    
    return '', 200


@app.route('/stream')
def stream():
    def event_stream():
        last_output = ''
        while True:
            try:
                current_output = tail(LOG_FILE, 200)
                if current_output != last_output:
                    # Split into multiple SSE data lines for each log line
                    sse_message = ''.join(f"data: {line}\n" for line in current_output.splitlines()) + '\n\n'
                    yield sse_message
                    last_output = current_output
            except Exception as e:
                yield f"data: Error reading file: {str(e)}\n\n"
            time.sleep(1)
    return Response(event_stream(), mimetype='text/event-stream')




@app.route("/events")
def events():
    """Provide real-time server updates via SSE."""
    return Response(event_stream(), content_type="text/event-stream")


@app.route('/publish', methods=['POST'])
def publish_message():
    data = request.json
    channel = data.get("channel")
    message = data.get("message")
    
    if not channel or not message:
        return jsonify({"error": "Channel and message are required."}), 400
    
    client.publish(channel, message)
    return jsonify({"success": True, "channel": channel, "message": message})


if __name__ == "__main__":

    setup_logger()

    # Run the Redis subscription in a separate thread
    threading.Thread(target=log_all_channels, daemon=True).start()

    
    #Initialize red to be able to call functions from dispatcher_monitor.py and action_server.py  
    red = redis.Redis(redishost)


    app.run(debug=True, host='0.0.0.0', port=3000, threaded=True)


# Run the following command to start server during production phase:
#  gunicorn -w 5 -k gevent --keep-alive 3600 --timeout 0 -b 0.0.0.0:5000 dispatcher_webmonitor:app
# 
# pip install gunicorn gevent
# #
# #

