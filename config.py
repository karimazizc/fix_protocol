import configparser
import json

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    return {
        'server': {
            'host': config['SERVER']['host'],
            'port': int(config['SERVER']['port']),
            'sender_comp_id': config['SERVER']['sender_comp_id'],
            'target_comp_id': config['SERVER']['target_comp_id']
        },
        'trading': {
            'heartbeat_interval': int(config['TRADING']['heartbeat_interval']),
            'symbols': config['TRADING']['symbols'].split(','),
            'reference_prices': json.loads(config['TRADING']['reference_prices']),
            'orders_per_symbol': int(config['TRADING']['orders_per_symbol']),
            'min_quantity': int(config['TRADING']['min_quantity']),
            'max_quantity': int(config['TRADING']['max_quantity']),
            'price_variance': float(config['TRADING']['price_variance']),
            'cancel_probability': float(config['TRADING']['cancel_probability']),
            'cancel_timeout': int(config['TRADING']['cancel_timeout'])
        },
        'timing': {
            'max_runtime': int(config['TIMING']['max_runtime']),
            'order_delay_min': float(config['TIMING']['order_delay_min']),
            'order_delay_max': float(config['TIMING']['order_delay_max'])
        }
    }


