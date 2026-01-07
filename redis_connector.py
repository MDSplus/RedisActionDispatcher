import os
import sys
import getpass
import redis


def add_redis_args(parser):
    g = parser.add_argument_group("redis")
    g.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    g.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    g.add_argument("--redis-db", type=int, default=int(os.getenv("REDIS_DB", "0")))

    g.add_argument("--redis-username", default=os.getenv("REDIS_USERNAME"))
    g.add_argument("--redis-password-file",
                   help="Read password from file, or '-' for stdin")
    g.add_argument("--prompt-password", action="store_true",
                   help="Prompt for password if authentication fails")

    g.add_argument("--tls", action="store_true")
    g.add_argument("--tls-ca-certs", default=os.getenv("REDIS_TLS_CA_CERTS"))

    g.add_argument("--redis-opt", action="append", default=[],
                   metavar="KEY=VALUE",
                   help="Extra redis.Redis kwargs (repeatable)")
    

def _parse_opts(opts):
    out = {}
    for s in opts:
        k, v = s.split("=", 1)
        vl = v.lower()
        if vl in ("true", "1", "yes", "on"):
            v = True
        elif vl in ("false", "0", "no", "off"):
            v = False
        else:
            try:
                v = int(v)
            except ValueError:
                try:
                    v = float(v)
                except ValueError:
                    pass
        out[k] = v
    return out


def _get_password(args):
    if args.redis_password_file:
        if args.redis_password_file == "-":
            return sys.stdin.read().strip() or None
        return open(args.redis_password_file, "r").read().strip() or None
    return os.getenv("REDIS_PASSWORD")


def _get_common_kwargs(host, port, db, username, password, tls, tls_ca_certs, extra_opts=None):
    kw = dict(
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
    )
    if extra_opts:
        kw.update(_parse_opts(extra_opts))

    if tls:
        kw["ssl"] = True
        if tls_ca_certs:
            kw["ssl_cert_reqs"] = "required"
            kw["ssl_ca_certs"] = tls_ca_certs
        else:
            kw["ssl_cert_reqs"] = "none"
    
    return kw


def connect_redis_from_args(args, *, prompt_on_auth_failure=False, **overrides):
    password = _get_password(args)

    kw = _get_common_kwargs(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        username=args.redis_username,
        password=password,
        tls=args.tls,
        tls_ca_certs=args.tls_ca_certs,
        extra_opts=args.redis_opt
    )

    kw.update(overrides)
    kw = {k: v for k, v in kw.items() if v is not None}

    r = redis.Redis(**kw)

    try:
        r.ping()
        return r
    except redis.exceptions.AuthenticationError:
        if prompt_on_auth_failure and args.prompt_password and sys.stdin.isatty():
            pw = getpass.getpass("Redis password: ")
            r = redis.Redis(**{**kw, "password": pw})
            r.ping()
            return r
        raise


def connect_from_env(**overrides):
    """
    Connect to Redis using configuration from environment variables.
    Supported variables:
      REDIS_HOST (default: localhost)
      REDIS_PORT (default: 6379)
      REDIS_DB (default: 0)
      REDIS_USERNAME
      REDIS_PASSWORD
      REDIS_TLS_CA_CERTS
      REDIS_TLS (true/1/yes/on to enable)
    """
    host = os.getenv("REDIS_HOST", "localhost")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    username = os.getenv("REDIS_USERNAME")
    password = os.getenv("REDIS_PASSWORD")
    
    tls_val = os.getenv("REDIS_TLS", "").lower()
    tls = tls_val in ("true", "1", "yes", "on")
    tls_ca_certs = os.getenv("REDIS_TLS_CA_CERTS")

    kw = _get_common_kwargs(
        host=host,
        port=port,
        db=db,
        username=username,
        password=password,
        tls=tls,
        tls_ca_certs=tls_ca_certs
    )

    kw.update(overrides)
    kw = {k: v for k, v in kw.items() if v is not None}

    r = redis.Redis(**kw)
    r.ping()
    return r

# -------------------------
# Example usage
# -------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    add_redis_args(parser)

    # Add your other arguments here...
    parser.add_argument("--channel", default="mychannel")

    args = parser.parse_args()

    r = connect_redis_from_args(args)
    print("Connected:", r.ping())
