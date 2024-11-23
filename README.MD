# FIX Protocol Client Implementation

A Python-based FIX (Financial Information eXchange) protocol client implementation that sends and manages orders for trading financial instruments.

## Features

- FIX Protocol 4.2 compliant
- Supports market and limit orders
- Real-time order management
- Automatic order cancellation
- Trading statistics calculation
- Heartbeat monitoring
- Comprehensive message logging

## Requirements

- Python 3.7+ (This script was created using python version 3.11.5)
- `simplefix` library
- Network connection to FIX server

the virtual environment folder `env` already has simplefix installed. User can simply use the environment to run the script without downloading simplefix.

## Usage

You can test the script by:

1. changing the config.ini host and port to:
```config.ini
[SERVER]
host = localhost
port = 5001
```

2. Run the server:
```bash
python server.py
```

3. Run the client:
```bash
python client.py
```

Or use the testing session provided by DTL's server

1. using the default config.ini host and port:
```config.ini
[SERVER]
host = fix.dytechlab.com
port = 5100
```

2. Run the client:
```bash
python client.py
```

The client will:
1. Connect to the FIX server
2. Send logon messge
3. Start sending random orders for configured symbols
4. Randomly cancel orders
5. Print trading statistics when complete

You can force stop the script by using ctrl + c and this will stop the execution for 5 seconds and then print 
the resulting trading statistics.

## Configuration

The config.ini comprises of the script's server, trading and timing. The script will send order by random between 0.1 to 0.5 seconds, with a maximum order of 1000 for MSFT, APPL and BAC along with its reference prices. This all can be customized by the user to align with the user's preferences.

## Message Types Supported

| Tag | Type | Description |
|-----|------|-------------|
| 35=A | Logon | Session initialization |
| 35=D | New Order | New single order entry |
| 35=F | Cancel Request | Order cancellation request |
| 35=8 | Execution Report | Order execution status |
| 35=3 | Reject | Order rejection |
| 35=9 | Cancel Reject | Cancellation rejection |
| 35=0 | Heartbeat | Connection health check |

Additionally, the user can use `describe_value()` method to give them all the key value pairs and their description.

Example usage: 
--- input --
`FIXCLIENT().describe_value(35)`

--- Output ---
tag 35 is the field's Message Type (MsgType)
35=A represent Logon
35=D represent NewOrder or Market Order msg type
35=F represent Order Cancel Request
35=0 represent Heartbeat
35=3 represent Reject Order
35=8 represent Execution Report
35=9 represent Order Cancel Reject

## Order Types

- Market Orders (40=1)
- Limit Orders (40=2)

## Order Sides

- Buy (54=1)
- Sell (54=2)
- Short Sell (54=5)

## Statistics Tracked

- Total trading volume (USD)
- Total PNL
- VWAP by symbol
- Orders count by symbol

## Class Structure

### Main Classes

- `FIXClient`: Core FIX protocol implementation
- `OrderStatus`: Order state tracking
- `TradeStatistics`: Trading metrics calculation
- `OrderType`: Order type enumeration
- `OrderSide`: Order side enumeration

### Key Methods

- `connect()`: Establish server connection
- `send_new_order()`: Submit new orders
- `send_cancel_request()`: Cancel existing orders
- `handle_execution_report()`: Process execution reports
- `print_statistics()`: Display trading metrics
- `message_reciever()`: Decode incoming messages
- `describe_value()`: Describe key value pairs information

## Error Handling

The client implements comprehensive error handling for:
- Connection failures
- Message parsing errors
- Unknown order IDs
- Rejected orders/cancellations
- Invalid messages

## Logging

Messages are logged with clear formatting:
```
------------------------SENT-------------------------
[Message content]

----------------------RECEIVED-----------------------
[Message content]
Received [N] messages
-----------------------ORDER-------------------------
[New order sent: SYMBOL ORDER_POSITION PRICE @ PRICE or MARKET]
[Cancel Request sent for order : ORDER ID]

------------------EXECUTION REPORT-------------------
[Order ORDER_ID filled/cancelled/rejected PRICE]
```

## Performance

- Uses threading for message handling
- Efficient message parsing
- Optimized network communication
- Performance timing decorator included

## Contact

For support or questions, contact author karimazizc@gmail.com