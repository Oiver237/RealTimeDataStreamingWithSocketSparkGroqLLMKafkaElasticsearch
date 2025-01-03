import json
import socket
import time
import pandas as pd

def send_data_over_socket(filepath, host = 'spark-master', port = 9999,chunk_size = 2):

    #AF_INET for ipv4 address et SOCK_STREAM for TCP
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', port))
    s.listen()

    print(f"Listenning for connections on {host}:{port}")


    last_sent_index = 0
    while True:
        conn, add= s.accept()
        print(f"Connection from {add}")
        try:
            with open(filepath, 'r') as file:
                #skip the lines that were already sent
                for _ in range(last_sent_index):
                    next(file)
                
                records = []
                for line in file:
                    records.append(json.loads(line))
                    if(len(records)) == chunk_size:
                        chunk = pd.DataFrame(records)
                        print(chunk)

                        for record in chunk.to_dict(orient='records'):
                            serialize_data = json.dumps(record).encode('utf-8')
                            conn.send(serialize_data + b'\n')
                            time.sleep(5)
                            last_sent_index+=1
                        records = []
        except (BrokenPipeError, ConnectionResetError):
            print('Client disconnected.')
        finally:
            conn.close()
            print('Connection closed')

                


if __name__ == "__main__":
    send_data_over_socket("datasets/yelp_academic_dataset_review.json")