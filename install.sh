#!/bin/bash
# Installer for RedisActionDispatcher systemd services
# Usage: sudo ./install.sh [install_user] [install_group] [install_path]

set -e

usage() {
        cat <<'EOF'
Usage: sudo ./install.sh [install_user] [install_group] [install_path]

Arguments (all optional):
    install_user   System user to own and run services (default: rdispatch)
    install_group  Group for files and services (default: rdispatch)
    install_path   Install location (default: /opt/rdispatch/RedisActionDispatcher)

Environment flags:
    SKIP_DISPATCHER=1   Skip installing action_dispatcher.service (non-interactive)
    SKIP_WEB=1          Skip installing dispatcher_webmonitor.service (non-interactive)

Interactive prompts:
    - Whether to install action_dispatcher.service (if SKIP_DISPATCHER not set)
    - Whether to install dispatcher_webmonitor.service (if SKIP_WEB not set)
    - Which action_server@ instances to enable/start (comma-separated class:id)

Examples:
    sudo ./install.sh                    # defaults with prompts
    SKIP_DISPATCHER=1 sudo ./install.sh  # non-interactive skip dispatcher
    SKIP_WEB=1 sudo ./install.sh         # non-interactive skip web monitor
EOF
}

if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
        usage
        exit 0
fi

INSTALL_USER="${1:-rdispatch}"
INSTALL_GROUP="${2:-rdispatch}"
INSTALL_PATH="${3:-/opt/rdispatch/RedisActionDispatcher}"
SKIP_DISPATCHER="${SKIP_DISPATCHER:-0}"
SKIP_WEB="${SKIP_WEB:-0}"
SYSTEMD_DIR="/etc/systemd/system"
ENV_FILE="/etc/rdispatch.env"

echo "==> RedisActionDispatcher Service Installer"
echo "    Install user: $INSTALL_USER"
echo "    Install group: $INSTALL_GROUP"
echo "    Install path: $INSTALL_PATH"
echo "    Skip action_dispatcher.service: $SKIP_DISPATCHER"
echo "    Skip dispatcher_webmonitor.service: $SKIP_WEB"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

# Check if user exists, create if needed
if ! id "$INSTALL_USER" &>/dev/null; then
    echo "==> Creating system user: $INSTALL_USER"
    useradd --system --shell /bin/bash --home-dir "$INSTALL_PATH" --create-home "$INSTALL_USER"
else
    echo "==> User $INSTALL_USER already exists"
fi

# Create install directory if needed
if [ ! -d "$INSTALL_PATH" ]; then
    echo "==> Creating install directory: $INSTALL_PATH"
    mkdir -p "$INSTALL_PATH"
fi

# Copy files if not installing in current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$SCRIPT_DIR" != "$INSTALL_PATH" ]; then
    echo "==> Copying files to $INSTALL_PATH"
    cp -r "$SCRIPT_DIR"/* "$INSTALL_PATH/"
    chown -R "$INSTALL_USER":"$INSTALL_GROUP" "$INSTALL_PATH"
else
    echo "==> Installing from current directory"
    chown -R "$INSTALL_USER":"$INSTALL_GROUP" "$INSTALL_PATH"
fi

# Create virtualenv if it doesn't exist
if [ ! -d "$INSTALL_PATH/.venv" ]; then
    echo "==> Creating Python virtual environment"
    sudo -u "$INSTALL_USER" python3 -m venv "$INSTALL_PATH/.venv"
    sudo -u "$INSTALL_USER" "$INSTALL_PATH/.venv/bin/python" -m pip install --upgrade pip
    sudo -u "$INSTALL_USER" "$INSTALL_PATH/.venv/bin/python" -m pip install -r "$INSTALL_PATH/requirements.txt"
    sudo -u "$INSTALL_USER" echo "/usr/local/mdsplus/python" > $INSTALL_PATH/.venv/lib/$(python3 --version 2>&1 | awk '{split($2,v,"."); print "python" v[1] "." v[2]}')/site-packages/mdsplus.pth
else
    echo "==> Virtual environment exists, updating dependencies"
    sudo -u "$INSTALL_USER" "$INSTALL_PATH/.venv/bin/python" -m pip install -r "$INSTALL_PATH/requirements.txt"
    sudo -u "$INSTALL_USER" echo "/usr/local/mdsplus/python" > $INSTALL_PATH/.venv/lib/$(python3 --version 2>&1 | awk '{split($2,v,"."); print "python" v[1] "." v[2]}')/site-packages/mdsplus.pth
fi

# Install environment file if it doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    echo "==> Installing environment file: $ENV_FILE"
    cp "$INSTALL_PATH/rdispatch.env.example" "$ENV_FILE"
    chmod 600 "$ENV_FILE"
    echo "    ⚠️  Edit $ENV_FILE to configure Redis connection"
else
    echo "==> Environment file $ENV_FILE already exists (not overwriting)"
fi

# Install systemd service files (prompt for web monitor if not predefined)
if [ -z "${SKIP_WEB+x}" ]; then
    read -r -p "Install dispatcher_webmonitor.service? [Y/n]: " _ans_web
    case "$_ans_web" in
        [nN]*) SKIP_WEB=1 ;;
        *) SKIP_WEB=0 ;;
    esac
fi

echo "==> Installing systemd service files"
for service_file in "$INSTALL_PATH"/*.service; do
    if [ -f "$service_file" ]; then
        service_name=$(basename "$service_file")
        if [ "$SKIP_DISPATCHER" = "1" ] && [ "$service_name" = "action_dispatcher.service" ]; then
            echo "    - skipping $service_name (SKIP_DISPATCHER=1)"
            continue
        fi
        if [ "$SKIP_WEB" = "1" ] && [ "$service_name" = "dispatcher_webmonitor.service" ]; then
            echo "    - skipping $service_name (SKIP_WEB=1)"
            continue
        fi
        echo "    - $service_name"
        # Substitute placeholders
        sed -e "s|%INSTALL_USER%|$INSTALL_USER|g" \
            -e "s|%INSTALL_PATH%|$INSTALL_PATH|g" \
            "$service_file" > "$SYSTEMD_DIR/$service_name"
        chmod 644 "$SYSTEMD_DIR/$service_name"
    fi
done

# Reload systemd
echo "==> Reloading systemd daemon"
systemctl daemon-reload

echo ""
echo "✅ Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit $ENV_FILE with your Redis configuration"
echo "  2. Enable and start services:"
echo "     systemctl enable --now action_dispatcher    # omit if SKIP_DISPATCHER=1"
echo "     systemctl enable --now action_server@my_server:1"
echo "     systemctl enable --now dispatcher_webmonitor  # omit if SKIP_WEB=1"
echo "  3. Check status:"
echo "     systemctl status action_dispatcher"
echo "     journalctl -u action_dispatcher -f"
echo ""
echo "Optional - Nginx reverse proxy:"
echo "  1. Install nginx: apt install nginx (Debian/Ubuntu) or yum install nginx (RHEL/CentOS)"
echo "  2. Copy config: cp $INSTALL_PATH/nginx-dispatcher.conf /etc/nginx/sites-available/dispatcher"
echo "  3. Edit server_name in /etc/nginx/sites-available/dispatcher"
echo "  4. Enable: ln -s /etc/nginx/sites-available/dispatcher /etc/nginx/sites-enabled/"
echo "  5. Test: nginx -t"
echo "  6. Reload: systemctl reload nginx"
echo ""
