import zmq, pickle, sys
from constPipe import IP_MID, PORT2
from collections import defaultdict

#  Formatação de saída 

SEPARATOR = "-" * 62

def print_result(item: dict, count: int):
    print(
        f"  #{count:04d} | "
        f"{item['type']:<7} | "
        f"{item['original']:>8.2f} {item['unit_in']:<4} "
        f"->  {item['converted']:>9.4f} {item['unit_out']:<3} "
        f"  [{item['worker']}]"
    )

def print_stats(stats: dict, total: int):
    print(f"\n{'='*62}")
    print(f"  ESTATISTICAS FINAIS  ({total} medicoes processadas)")
    print(f"{'='*62}")
    for mtype, data in stats.items():
        avg = data["sum"] / data["count"]
        print(
            f"  {mtype:<7}  "
            f"  n={data['count']:>4}  "
            f"  media={avg:>9.4f} {data['unit_out']}"
            f"  min={data['min']:>9.4f}  max={data['max']:>9.4f}"
        )
    print(f"{'='*62}\n")


#  Consumer principal 

def run_sink():
    context = zmq.Context()
    socket  = context.socket(zmq.PULL)              # socket de recepção
    addr    = f"tcp://{IP_MID}:{PORT2}"
    socket.connect(addr)                            # conecta ao middleware

    print(f"[SINK] Conectado ao Middleware em {addr}")
    print(f"[SINK] Aguardando dados convertidos…\n")
    print(SEPARATOR)

    # Acumula estatísticas por tipo
    stats   = defaultdict(lambda: {"count": 0, "sum": 0.0,
                                   "min": float("inf"), "max": float("-inf"),
                                   "unit_out": ""})
    total   = 0

    try:
        while True:
            if not socket.poll(3000):               # timeout 3 s
                print("[SINK] Aguardando…")
                continue

            raw  = socket.recv()
            item = pickle.loads(raw)

            if item.get("type") == "STOP":
                print(f"\n[SINK] Sinal de STOP recebido.")
                break

            total += 1
            print_result(item, total)

            # Atualiza estatísticas
            mtype = item["type"]
            v     = item["converted"]
            s     = stats[mtype]
            s["count"]    += 1
            s["sum"]      += v
            s["unit_out"]  = item["unit_out"]
            s["min"]       = min(s["min"], v)
            s["max"]       = max(s["max"], v)

    except KeyboardInterrupt:
        print("\n[SINK] Interrompido pelo usuário.")
    finally:
        print_stats(stats, total)
        socket.close()
        context.term()
        print("[SINK] Encerrado.")


if __name__ == "__main__":
    run_sink()
