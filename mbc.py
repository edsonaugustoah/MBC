import asyncio
import pymodbus.client as ModbusClient
from pymodbus.exceptions import ModbusIOException
import getmac
import time
import firebase_admin
from firebase_admin import credentials, db, auth

# Definição de variáveis
host = "192.168.1.10"
port = 502

# Autenticação Firebase
cred = credentials.Certificate("esp32.json")
firebase_admin.initialize_app(cred, {'databaseURL': 'https://esp32-001-b5578-default-rtdb.firebaseio.com/'})

# Obter o MAC address
mac_address = getmac.get_mac_address().replace(":", "")
print("Endereco MAC: ", mac_address)

# Fazer login com email e senha
try:
    user = auth.get_user_by_email("mestre@mestre.com")
    print("Autenticação bem-sucedida!")
except auth.AuthError as e:
    print(f"Erro de autenticação: {e}")

async def write_registers(client, register, value):
    # Escrever no registrador especificado
    client.write_registers(register, [value], slave=1)

async def read_register(client, register):
    try:
        # Ler o valor do registrador especificado
        result = client.read_holding_registers(register, 1, slave=1)
        return result.registers[0]
    except ModbusIOException:
        print(f"Erro na leitura do registrador {register}")
        return None

async def run_modbus_client():
    # Criar cliente Modbus TCP assíncrono
    client = ModbusClient.ModbusTcpClient(host, port=port)

    try:
        # Conectar ao servidor Modbus
        client.connect()

        # Testar a conexão
        if not client.is_socket_open():
            raise Exception("A conexão com o servidor Modbus não foi estabelecida com sucesso.")

        last_execution_time = 0

        while True:
            current_time = int(time.time() * 1000)

            # Verificar se passaram pelo menos 10 segundos desde a última execução
            if current_time >= last_execution_time + 10000:
                timestamp = current_time

                # Obter a referência para a pasta de registradores
                registradores_ref = db.reference(f"Registradores/{mac_address}")

                print("Conseguiu acessar")

                idRegistradores = registradores_ref.get()
                if idRegistradores:
                    registros = {}

                    for register_number in idRegistradores:
                        try:
                            # Verificar se é permitido ler o registrador
                            is_input_allowed_ref = db.reference(f"Registradores/{mac_address}/{register_number}/isInput")
                            is_input_allowed = is_input_allowed_ref.get()

                            if is_input_allowed is not None and not is_input_allowed:
                                value = await read_register(client, int(register_number))
                                if value is not None:
                                    data = {
                                        "data": value,
                                        "time": timestamp
                                    }

                                    # Criar referência específica para o registrador atual
                                    register_ref = db.reference(f"Registros/{mac_address}/{register_number}")
                                    register_ref.update({timestamp: data})
                        except Exception as e:
                            print(f"Erro ao processar registrador {register_number}: {e}")

                # Atualizar o tempo da última execução
                last_execution_time = current_time

            # Aguardar um curto período antes de verificar novamente
            await asyncio.sleep(0.1)

    except Exception as e:
        print(f"Erro durante a execução: {e}")

    finally:
        # Aguardar um curto período antes de fechar a conexão
        await asyncio.sleep(0.1)

        # Fechar a conexão se estiver aberta
        if client.is_socket_open():
            client.close()

if __name__ == "__main__":
    asyncio.run(run_modbus_client())
