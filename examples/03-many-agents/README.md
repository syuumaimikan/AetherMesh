# 03 · Many agents on one machine

Four workers on one box: enough to watch the scheduler make choices, without
owning four computers.

## Start them

```bash
./run.sh 4          # macOS, Linux
```

```powershell
.\run.ps1 -Agents 4   # Windows
```

Each agent needs its own identity file. Without `--identity-path` they would
all read the same one and register as the *same* node — the mesh would look
like one machine that keeps reconnecting.

## Watch the placement

```bash
python ../../sdk/python/examples/hash.py
```

The first task carrying the dataset picks a node; the rest follow it there,
because the locality bonus outweighs the small load difference between four
idle agents. Load one of them down and the balance changes:

```bash
python -c "
import sys; sys.path.insert(0, '../../sdk/python')
from aethermesh import AetherMesh
with AetherMesh.connect(port=7100) as mesh:
    for _ in range(20):
        print(mesh.run('cpu', (5_000_000).to_bytes(8, 'little')).node_id[:8])
"
```

The node ids stop being all the same once the busy one's CPU shows up in a
heartbeat: `compute_cost` rises, and the score sends work elsewhere.

## Clean up

```bash
./stop.sh
```

```powershell
.\stop.ps1
```
