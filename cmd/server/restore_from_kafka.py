import json

def restore(x):
    try:
        msg = x['value'].decode('utf-8')
        event = json.loads(msg)

        station = event['station']
        delta = int(event['delta'])

        # This is the magic: apply the exact same delta that happened originally
        execute('HINCRBY', f'station:{station}', 'stock', str(delta))

        # Optional: ensure geo index exists (only once per station)
        if not execute('HEXISTS', f'station:{station}', 'lat'):
            # If you lose the hash completely, you need a way to restore lat/lon/name
            # For now, just log (or later add a separate seed topic)
            print(f"Warning: station:{station} missing lat/lon – stock restored only")
    except Exception as e:
        print("Gears restore error:", str(e))

# Subscribe to Kafka topic and replay from beginning on new Redis instance
gb = GearsBuilder('KafkaStreamReader',
                  servers='kafka:9092',
                  topic='station-updates',
                  group='redis-gears-restore',
                  fromBeginning=True)   # <-- THIS replays all history!

gb.foreach(restore)
gb.run()