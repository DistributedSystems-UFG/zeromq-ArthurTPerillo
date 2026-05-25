"""
Conecta-se ao Publisher, assina os tópicos desejados e exibe as leituras
convertidas de unidades imperiais para o Sistema Internacional.

Tópicos disponíveis:
  TEMP    Fahrenheit  → Celsius
  WEIGHT  Libras      → kg
  DIST    Milhas      → km
  LENGTH  Polegadas   → cm

Uso:
  python zmq_subscriber.py [ip_pub] [porta] [TOPICO1 TOPICO2 ...]

  Exemplos:
    python zmq_subscriber.py 0.0.0.0 5678 TEMP WEIGHT
    python zmq_subscriber.py 0.0.0.0 5678         # assina todos os tópicos
"""

import zmq, sys, time
from collections import defaultdict

#  Conversões Imperial → SI 

CONVERSIONS = {
    "TEMP"   : (lambda f: (f - 32) * 5 / 9,  "°F",  "°C"),
    "WEIGHT" : (lambda x: x * 0.453592,       "lbs", "kg"),
    "DIST"   : (lambda x: x * 1.60934,        "mi",  "km"),
    "LENGTH" : (lambda x: x * 2.54,           "in",  "cm"),
}

ALL_TOPICS = list(CONVERSIONS.keys())

#  Parse e conversão de mensagem 

def parse_and_convert(raw: str) -> dict | None:
    """
    Mensagem esperada: "TOPICO valor unidade timestamp"
    Retorna dict com original e convertido, ou None se inválida.
    """
    parts = raw.strip().split()
    if len(parts) != 4:
        return None

    topic, value_str, unit_in, ts = parts
    if topic not in CONVERSIONS:
        return None

    try:
        value = float(value_str)
    except ValueError:
        return None

    fn, _, unit_out = CONVERSIONS[topic]
    converted       = round(fn(value), 4)

    return {
        "topic"    : topic,
        "original" : value,
        "unit_in"  : unit_in,
        "converted": converted,
        "unit_out" : unit_out,
        "ts"       : ts,
    }


def format_reading(d: dict, count: int) -> str:
    return (
        f"  [{d['ts']}]  #{count:04d}  "
        f"{d['topic']:<7}  "
        f"{d['original']:>8.2f} {d['unit_in']:<4}"
        f"  ->  {d['converted']:>9.4f} {d['unit_out']}"
    )


def print_stats(stats: dict, total: int):
    if total == 0:
        return
    print(f"\n{'═'*64}")
    print(f"  RESUMO FINAL  ({total} leituras recebidas)")
    print(f"{'═'*64}")
    for topic, s in stats.items():
        if s["count"] == 0:
            continue
        avg = s["sum"] / s["count"]
        fn, _, uo = CONVERSIONS[topic]
        print(
            f"  {topic:<7}  "
            f"n={s['count']:>3}  "
            f"media={avg:>9.4f} {uo}  "
            f"min={s['min']:>9.4f}  max={s['max']:>9.4f}"
        )
    print(f"{'═'*64}\n")


#  Subscriber principal 

def run_subscriber(pub_ip: str = "localhost", port: int = 12345,
                   topics: list[str] | None = None):

    if not topics:
        topics = ALL_TOPICS                 # sem filtro → assina tudo

    context = zmq.Context()
    socket  = context.socket(zmq.SUB)      # socket assinante
    addr    = f"tcp://{pub_ip}:{port}"
    socket.connect(addr)                   # conecta ao publisher

    # Assina cada tópico escolhido
    for t in topics:
        socket.setsockopt(zmq.SUBSCRIBE, t.encode())
        print(f"[SUB] Inscrito em: {t}  ({CONVERSIONS[t][1]} → {CONVERSIONS[t][2]})")

    print(f"[SUB] Conectado em {addr}\n")
    print("─" * 64)

    stats = defaultdict(lambda: {"count": 0, "sum": 0.0,
                                 "min": float("inf"), "max": float("-inf")})
    total = 0

    try:
        while True:
            raw = socket.recv().decode()        # aguarda mensagem do tópico
            d   = parse_and_convert(raw)
            if d is None:
                continue

            total += 1
            print(format_reading(d, total))

            # Atualiza estatísticas
            s = stats[d["topic"]]
            v = d["converted"]
            s["count"] += 1
            s["sum"]   += v
            s["min"]    = min(s["min"], v)
            s["max"]    = max(s["max"], v)

    except KeyboardInterrupt:
        print("\n[SUB] Interrompido.")
    finally:
        print_stats(stats, total)
        socket.close()
        context.term()
        print("[SUB] Encerrado.")


if __name__ == "__main__":
    # Argumentos: ip porta TOPICO1 TOPICO2 ...
    ip     = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port   = int(sys.argv[2]) if len(sys.argv) > 2 else 5678
    topics = [t.upper() for t in sys.argv[3:]] if len(sys.argv) > 3 else []

    # Valida tópicos fornecidos
    invalid = [t for t in topics if t not in ALL_TOPICS]
    if invalid:
        print(f"[SUB] Tópicos inválidos: {invalid}")
        print(f"[SUB] Disponíveis: {ALL_TOPICS}")
        raise SystemExit(1)

    run_subscriber(ip, port, topics)
