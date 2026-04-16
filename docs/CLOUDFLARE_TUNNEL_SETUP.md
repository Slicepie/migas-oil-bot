# Cloudflare Tunnel — `api.usoil.ai`

Goal: stable `https://api.usoil.ai` URL that survives RunPod stop/start and IP changes.

## One-time dashboard setup (~5 min)

### 1. Remove the existing A record

In Cloudflare dashboard → `usoil.ai` → **DNS**:

- Delete the current `api` A record pointing at `103.196.86.91`. (The tunnel will create a CNAME automatically.)

### 2. Create the tunnel

In Cloudflare dashboard → **Zero Trust** → **Networks** → **Tunnels** → **Create a tunnel**:

1. Connector type: **Cloudflared**
2. Tunnel name: `usoil-api`
3. Click **Save tunnel**
4. On the next screen ("Install and run a connector"):
   - **Don't** copy the curl install command — our `entrypoint.sh` handles that.
   - **Do** copy the long token from the install command (everything after `--token `). It looks like `eyJhIjoi...` and is ~200 chars.

### 3. Add the public hostname

Still in the tunnel setup, click **Next** → **Public Hostnames** tab → **Add a public hostname**:

- Subdomain: `api`
- Domain: `usoil.ai`
- Type: `HTTP`
- URL: `localhost:8080`

Click **Save hostname**.

Cloudflare automatically creates a CNAME `api.usoil.ai → <tunnel-uuid>.cfargotunnel.com` with proxy ON.

### 4. Set the token in RunPod

In RunPod → your pod → **Edit** → **Environment Variables** → add:

```
CLOUDFLARE_TUNNEL_TOKEN = <paste the token from step 2>
```

Save. **Stop + Start** the pod (not Restart — entrypoint only re-runs on fresh start).

### 5. Verify

After ~30s, from your Mac:

```bash
curl https://api.usoil.ai/api/v1/market/price
```

Should return `{"symbol":"CL","price":...}` over real TLS, no port number.

Check pod logs for:
```
[startup] cloudflared installed: cloudflared version ...
[startup] Tunnel started (log: /app/cloudflared.log)
```

If something's wrong:
```bash
tail -50 /app/cloudflared.log
```

## What this gives you

- ✅ `https://api.usoil.ai` — clean URL, real TLS
- ✅ Survives RunPod stop/start and pod-host migrations
- ✅ DDoS / WAF protection from Cloudflare
- ✅ Free tier — no recurring cost

## After it works — flip the skill default

Update `hermes-skill/usoil/scripts/_lib.sh` and `SKILL.md` from:
```
http://api.usoil.ai:34412
```
to:
```
https://api.usoil.ai
```

Then commit + push, and the skill is ready to publish.
