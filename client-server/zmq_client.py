import zmq
import json
import sys

#  Menu de conversões 

MENU = """
   SISTEMA DE CONVERSÃO  (Imperial→SI)    
    [1] Temperatura   Fahrenheit  → Celsius 
    [2] Peso          Libras      → kg      
    [3] Distância     Milhas      → km      
    [4] Comprimento   Polegadas   → cm      
    [0] Encerrar servidor e sair            

"""

TYPE_MAP = {
    "1": ("temp",   "Fahrenheit (°F)"),
    "2": ("weight", "Libras (lbs)"),
    "3": ("dist",   "Milhas (mi)"),
    "4": ("length", "Polegadas (in)"),
}

#  Helpers de I/O 

def send_request(socket, payload: dict) -> dict:
    """Serializa, envia e desserializa a resposta."""
    socket.send(json.dumps(payload).encode())
    raw  = socket.recv()
    return json.loads(raw.decode())


def format_response(resp: dict) -> str:
    if resp.get("status") == "ok":
        return (
            f"  Resultado: {resp['input']} {resp['unit_in']}"
            f" = {resp['output']} {resp['unit_out']}"
        )
    return f"  ✗ Erro: {resp.get('message', 'desconhecido')}"

# Loop principal 

def run_client(server_ip: str = "0.0.0.0", port: int = 5678):
    context = zmq.Context()
    socket  = context.socket(zmq.REQ)               # socket de requisição
    addr    = f"tcp://{server_ip}:{port}"
    socket.connect(addr)                            # conecta ao servidor
    print(f"[CLIENT] Conectado em {addr}\n")

    try:
        while True:
            print(MENU)
            choice = input("Escolha uma opção: ").strip()

            if choice == "0":                       # encerrar
                print("[CLIENT] Enviando STOP ao servidor…")
                socket.send(b"STOP")
                resp = json.loads(socket.recv())
                print(f"[CLIENT] Servidor respondeu: {resp.get('message')}")
                break

            if choice not in TYPE_MAP:
                print("  Opção inválida. Tente novamente.")
                continue

            conv_type, label = TYPE_MAP[choice]

            try:
                raw_val = input(f"  Digite o valor em {label}: ").strip()
                value   = float(raw_val)
            except ValueError:
                print("  Valor inválido. Digite um número.")
                continue

            payload = {"type": conv_type, "value": value}
            resp    = send_request(socket, payload)
            print(format_response(resp))

    except KeyboardInterrupt:
        print("\n[CLIENT] Interrompido pelo usuário.")
    finally:
        socket.close()
        context.term()
        print("[CLIENT] Conexão encerrada.")


if __name__ == "__main__":
    ip   = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 12345
    run_client(ip, port)
