RedisActionDispatcher
===
New distributed implementation of MDSplus dispatcher based on REDIS
---
<br>
This is a new implementation of the MDSplus action dispatcher based on redis. The new implementation is written entirely in Python and uses REDIS as a communication medium between the central dispatcher and the action servers. The dispatcher carries out the overall synchronization, ensuring that sequential actions are executed in the proper sequence (per server instance) and dependent actions are executed when their associated condition is met. The server class name is passed as an argument to action servers in addition to a unique ID number (usually set to 1). It is, in fact, possible to launch more than one action server per server class to carry out parallel execution of actions with the same server class. Action servers will carry out the assigned tasks and notify the dispatcher via redis when the action has terminated. Pending actions can be aborted, and the associated terminal output is captured and saved in redis so that it can be later visualized (see the Web interface description).
<br>
To launch the action dispatcher: <br>

```
python action_dispatcher.py [redis server]
```
Where redis server is the IP address of the machine hosting the redis server. If not defined, localhost is assumed <br>
To launch an action server: <br>

```
  python action_server.py <server_class> <server_id> <redis_server>
```
  
Any number of action servers can be launched in a distributed system, provided that the redis_server argument points to the same IP (the machine hosting the redis server). <br>
Once a dispatcher and a set of servers have been launched, it is possible to interact with the system via the following command: <br>

```
python dispatcher_commands.py <redis_sever> <command>
```
where <command> is one of the following:

-  **quit**: quit dispatcher
- **abort**: abort the current sequence
- **create_pulse <tree> <shot>**: create a new pulse file
- **build_tables**: create dispatch tables for the current pulse file (defined by create_pulse)
- **do_phase <phase>**: execute the actions defined for the specified phase
- **server_restart <server class> <server id>**: abort pending actions and restart server')
- **server_abort <server class> <server id> <action path>**: abort specified actions for the specified server
- **server_stop <server class> <server id>**: stop server (after finishing pending actions)
- **server_quit <server class> <server id>**: abrupt quit server
- **print_pending**: let the dispatcher print pending action (for debug purpose)

It is possible to print the terminal output during execution for a given action server and possibly a given action:

```
 python display_logs.py <tree> <shot> <server class> <action full path> <redis server>
```
where the action full path refers to the path in the tree of the target action. If ANY is specified, then the terminal output of all actions for that server class is displayed during execution.
<br><br>
# Web interface
Even if it is possible to interact with the action dispatcher via program dispatcher_commands.py, the preferred interface is web-based. 
<br>
The web application provides real-time visibility and control over distributed actions and servers using a Redis-backed architecture.

It is intended for:

* System operators
* Developers
* Engineers overseeing MDSPlus Redis Dispatcher automated processes

---

## Main Tabs Overview

| Tab                  | Purpose                                                                                    |
| -------------------- | ------------------------------------------------------------------------------------------ |
| **Server Monitor**   | View the real-time status (ON/OFF) of all active servers and perform basic server actions. |
| **Actions Monitor**  | Monitor action status by tree/shot and dispatch or abort actions.                          |
| **Log Viewer**       | Submit log messages manually for testing and stream live logs from the server log file.    |
| **Server HeartBeat** | View the latest heartbeat activity timestamp for each server.                              |

---

## Server Monitor
![ServerMonitor](images/server_monitor.png)
The **Server Monitor** tab provides:

* Live status of all servers registered in Redis (`ACTION_SERVER_ACTIVE`)
* Server status display (`ON` / `OFF`)
* Server control buttons:

  * `START`
  * `QUIT`
  * `RESTART`
  * `KILL`
* Automatic updates of server information
* Priority sorting where active (`ON`) servers appear first

### Notes

* The control buttons `START` and `RESTART` invoke scripts that  implement project-specific actions. These actions will launch the Python program action_server.py on the target, system-specific,  machines. These operations are carried out by the Web interface by invoking the following shell script:

```
sh server_command.sh {server_class} {command}   
```
where command can be either START or RESTART. It is therefore sufficient to provide such a shell script to adapt the Web interface to the specific project application. 

## Actions Monitor

The **Actions Monitor** tab displays two tables:

1. **Active Actions** (based on `CURRENT_PHASE`)
2. **All Actions** (regardless of phase)

### Action Information

Each row includes:

* Tree
* Shot
* Server
* Node
* State
* Status
* Phase

### Available Actions

Each row supports the following operations:

* `DISPATCH`
* `ABORT`
* `LOGS`

The **LOGS** button accesses the latest action log information stored in Redis.

### Additional Features

* Dispatcher form for:

  * Pulse creation
  * Table builds
  * Custom commands

### Color Coding

| Color  | Meaning           |
| ------ | ----------------- |
| Blue   | Not Dispatched    |
| Green  | Success           |
| Red    | Failed or Aborted |
| Orange | In Progress       |
| Grey   | Server Off        |

### Live Summary

A live summary displays:

* Total actions
* Categorized status counts

Failed and aborted actions are displayed first.

---

## Log Viewer

The **Log Viewer** tab provides:

* Submission of custom log messages for testing
* Live streaming of the `redis_pubsub.log` file
* Auto-scrolling log updates

### Additional Logging

Other logs can be redirected to the log file configured in the Flask application running on the server.

This allows project-specific logs to appear in the Log Viewer tab.

---

## Server Heartbeat

The **Server Heartbeat** tab:

* Displays the latest known heartbeat timestamp for each server
* Updates when receiving messages from:

```text
ACTION_SERVER_PUBSUB:*
```

---

## Usage Tips

* Use `DISPATCH` only after `create_pulse` and `build_tables` have been executed.
* Use logs to debug Redis messaging or command execution failures.
* Monitor the **Active Actions** table for work currently in progress.
* Use the **Heartbeat** tab to diagnose active server polling status.

---

## Starting the Web Server

Install the required dependencies:

```bash
pip install gunicorn gevent
```

Launch the web server using:

```bash
gunicorn -w 5 -k gevent --keep-alive 3600 --timeout 0 -b 0.0.0.0:5000 dispatcher_webmonitor:app
```

## Gunicorn Options

| Option                      | Description                                          |
| --------------------------- | ---------------------------------------------------- |
| `-w 5`                      | Start 5 worker processes                             |
| `-k gevent`                 | Use the Gevent worker class for asynchronous support |
| `--keep-alive 3600`         | Keep connections alive for up to 3600 seconds        |
| `--timeout 0`               | Disable worker timeout                               |
| `-b 0.0.0.0:5000`           | Bind the server to all interfaces on port 5000       |
| `dispatcher_webmonitor:app` | Python module and Flask app instance to serve        |

---
