from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import json
import sys, time, msvcrt, selectors
import base64 #redundant

# Define ENDPOINT, CLIENT_ID, PATH_TO_CERTIFICATE, PATH_TO_PRIVATE_KEY, PATH_TO_AMAZON_ROOT_CA_1, MESSAGE, TOPIC, and RANGE
ENDPOINT = "a10lrrmt9cmd0k-ats.iot.eu-north-1.amazonaws.com"
CLIENT_ID = "my_pc"
PATH_TO_CERTIFICATE = "certificates/certificate.pem.crt.txt"
PATH_TO_PRIVATE_KEY = "certificates/private.pem.key"
PATH_TO_AMAZON_ROOT_CA_1 = "certificates/root.pem"
MESSAGE = "Hello World"
TOPIC = "test/testing"
RANGE = 20
measurement_data=[]
ping_data=[]
lora_ping_holder=[]
id_lora_device="60C5A8FFFE79945B"



def publish(message, topic):
    mqtt_connection.publish(topic=topic, payload=message, qos=mqtt.QoS.AT_LEAST_ONCE)
    print("Published: '" + message + "' to the topic: " + topic)
    return 0

#takes two bytes and returns a single integer value
def decipher_two_bytes(byte1, byte2):
    byte1_as_byte=int(byte1).to_bytes(1, 'big')
    byte2_as_byte = int(byte2).to_bytes(1, 'big')
    bytes_combined=byte1_as_byte+byte2_as_byte
    real_number=int.from_bytes(bytes_combined, 'big')
    return real_number

def create_power_price_list():
    list_prices_2 = [0.0534, 0.0856, 0.0645, 0.0613, 0.0632, 0.0632, 0.0999, 0.1342, 0.1232, 0.1111, 0.1034, 0.1321,
                     0.1203, 0.1233, 0.1211, 0.1430, 0.1233, 0.3512, 0.1643, 0.1313, 0.1210, 0.1130, 0.1001, 0.0998]
    p_list=[250]
    for element in list_prices_2:
        as_bytes=int(element*1000).to_bytes(2, 'big')
        p_list.append(int.from_bytes(as_bytes[0:1], 'big'))
        p_list.append(int.from_bytes(as_bytes[1:2], 'big'))
    return p_list


#old version dont use!
def sent_to_lora(id, payload):

    if payload>255 or payload<0:
        print("one of the elements in the payload is out of range (0 to 255)")
        return 0
    topic = "lorawan/downlink"
    my_payload=""
    try:
        my_payload=base64.b64encode(payload.to_bytes(1, 'big')).decode('ascii')
    except:
        print("could not format payload")
        return 0
    to_send = {"thingName": id, "bytes": my_payload}
    json_format = json.dumps(to_send)
    print('sending {} to lorawan device {}'.format(payload, id))
    publish(json_format, topic)
    return 0


#payload is a list in form [11, 11, 11]
#sends a message tho specified thing on lora network
def send_to_lora_v2(thing_id, payload_as_list):
    for element in payload_as_list:
        if element>255 or element<0:
            print("one of the elements in the payload is out of range (0 to 255)")
            return 0
    # to_send = {"thingName": thing_id, "bytes": my_payload}
    to_send = {"thingName": thing_id, "bytes": payload_as_list}
    json_format = json.dumps(to_send)
    print('sending {} to lorawan device {}'.format(str(payload_as_list), thing_id))
    publish(json_format, "lorawan/downlink")
    return 0

#is run upon receiving message on the topic "metering"
def metering_handler(topic, payload, dup, qos, retain, **kwargs):
    #dateTimeObj = time.perf_counter()
    try:
        print_message(topic, payload)
        print('Size of payload: {}'.format(sys.getsizeof(json_format)))
        pass
    except:
        pass
    #ping_delay = (dateTimeObj - ping_data[0]) * 1000
    #print('delay in ms: ', end='')
    #print(ping_delay)
    return 1

#returns the payload in list format (for lora payloads)
def get_bytes_from_payload(payload):
    json_object=json.loads(payload)
    json_data=json_object['uplink_message']['decoded_payload']['bytes']
    #print(type(json_data))
    return json_data


#gets and returns the lora ID in the message, returns unknown if it fails
def get_id_from_payload(payload):
    try:
        json_object=json.loads(payload)
        json_data=json_object['end_device_ids']['device_id']
        json_data=json_data[4:]
        #print(type(json_data))
    except:
        json_data="unkown"
    return json_data

list_of_delay_lora=[]
#is called upon when a message is posted in Lora/uplink topic
def lorawan_uplink_handler(topic, payload, dup, qos, retain, **kwargs):
    try:
        dateTimeObj = time.perf_counter()
        print_message(topic, payload)
        payload=payload.decode("utf-8")
        #print(payload)
        #print(type(payload))
        rec_device_id= get_id_from_payload(payload)
        received_bytes=get_bytes_from_payload(payload)
        print(received_bytes)

        #never gets called
        if received_bytes[0]==2000:
            topic = "lorawan/downlink"
            to_send = {"thingName": "60C5A8FFFE79945B", "bytes": "AQ=="}
            json_format = json.dumps(to_send)
            #publish(json_format, topic)
            pass
        elif received_bytes[0]==111:
            print('lorawan device {} online'.format(rec_device_id))

        elif received_bytes[0] == 24: #temperature data received
            print('temperature is:{}'.format(received_bytes[1]))

        elif received_bytes[0] == 25: #metering data received
            usage_data=int(received_bytes[1]).to_bytes(1, 'big')+int(received_bytes[2]).to_bytes(1, 'big')
            usage_data= int.from_bytes(usage_data, "big")
            print('usage:{} KWh'.format(usage_data))

        elif received_bytes[0] == 26: #ping received
            ping_delay = (dateTimeObj-lora_ping_holder[0])*1000
            print('delay in ms: ', end='')
            print(ping_delay)
            list_of_delay_lora.append(int(ping_delay))
            print('lora delay list:', end='')
            print(list_of_delay_lora)
            #print('received ping')
        elif received_bytes[0] == 30:  # metering entire day
            day=decipher_two_bytes(received_bytes[1], received_bytes[2])
            list_meas=[]
            for i in range(3, 51, 2):
                measurement_as_wh = decipher_two_bytes(received_bytes[i], received_bytes[i+1])
                meas_as_kwh=measurement_as_wh/1000
                list_meas.append(meas_as_kwh)
            print('meterdata for day: {} :'.format(day))
            print(list_meas)

        elif received_bytes[0] == 31:  # metering one hour
            day = decipher_two_bytes(received_bytes[1], received_bytes[2])
            hour = received_bytes[3]
            measurement_as_wh = decipher_two_bytes(received_bytes[4], received_bytes[5])
            meas_as_kwh = measurement_as_wh / 1000
            print('measurement point with id:{} reported {} KWh on day:{}, hour:{}'.format(rec_device_id, meas_as_kwh,
                                                                                           day, hour))

        elif received_bytes[0] == 32:  # send back electricity price
            prices=create_power_price_list()
            send_to_lora_v2(rec_device_id, prices)
        else:
            print('nothing done in lorawan_uplink_handler()')
    except:
        print('exception in lorawan_uplink_handler')
        pass
    return 1

def lorawan_downlink_handler(topic, payload, dup, qos, retain, **kwargs):
    try:
        print_message(topic, payload)
    except:
        print('exception in lorawan_downlink_handler')
        pass
    return 1

def temperature_handler(topic, payload, dup, qos, retain, **kwargs):
    try:
        print_message(topic, payload)
    except:
        print('exception in temperature_handler')
        pass
    return 1

list_of_delay_nb=[]
def ping_handler(topic, payload, dup, qos, retain, **kwargs):
    try:
        dateTimeObj = time.perf_counter()
        print(dateTimeObj)
        print_message(topic, payload)
        ping_delay=(dateTimeObj-ping_data[0])*1000
        print('delay in ms: ', end='')
        print(ping_delay)
        list_of_delay_nb.append(int(ping_delay))
        print('nb delay list:', end='')
        print(list_of_delay_nb)
    except:
        print('exception in temperature_handler')
        pass
    return 1

def print_message(topic, payload):
    try:
        print("Received a new message from topic: ", topic)
        print(payload.decode('utf-8'))
    except:
        print('failed to print')
        pass

start_time = time.perf_counter()

# Spin up resources
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERTIFICATE,
            pri_key_filepath=PATH_TO_PRIVATE_KEY,
            client_bootstrap=client_bootstrap,
            ca_filepath=PATH_TO_AMAZON_ROOT_CA_1,
            client_id=CLIENT_ID,
            clean_session=False,
            keep_alive_secs=6
            )


print("Connecting to {} with client ID '{}'...".format(ENDPOINT, CLIENT_ID))
# Make the connect() call
connect_future = mqtt_connection.connect()
# Future.result() waits until a result is available
connect_future.result()

print("Connected!")
if False:
    topic = "general/test"
    to_send = {"thingName": "60C5A8FFFE79945B", "bytes": "AQ=="}
    json_format = json.dumps(to_send)
    publish(json_format, topic)


TIMEOUT = 10

#subscribing to all of the topics
subscribe_future_1, packet_id_1 = mqtt_connection.subscribe(
        #topic="lorawan/uplink",
        topic="lorawan/60C5A8FFFE79945B/uplink",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=lorawan_uplink_handler)
subscribe_result_1 = subscribe_future_1.result()
print("Subscribed with {} to {}".format(str(subscribe_result_1['qos']), str(subscribe_result_1['topic']) ))

subscribe_future_2, packet_id_2 = mqtt_connection.subscribe(
        topic="lorawan/downlink",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=lorawan_downlink_handler)
subscribe_result_2 = subscribe_future_2.result()
print("Subscribed with {} to {}".format(str(subscribe_result_2['qos']), str(subscribe_result_2['topic']) ))

subscribe_future_3, packet_id_3 = mqtt_connection.subscribe(
        topic="general/nb/metering",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=metering_handler)
subscribe_result_3 = subscribe_future_3.result()
print("Subscribed with {} to {}".format(str(subscribe_result_3['qos']), str(subscribe_result_3['topic']) ))

subscribe_future_4, packet_id_4 = mqtt_connection.subscribe(
        topic="general/nb/temperature",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=temperature_handler)
subscribe_result_4 = subscribe_future_4.result()
print("Subscribed with {} to {}".format(str(subscribe_result_4['qos']), str(subscribe_result_4['topic']) ))

subscribe_future_5, packet_id_5 = mqtt_connection.subscribe(
        topic="general/nb/ping",
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=ping_handler)
subscribe_result_5 = subscribe_future_5.result()
print("Subscribed with {} to {}".format(str(subscribe_result_5['qos']), str(subscribe_result_5['topic']) ))

end_time = time.perf_counter()
print('time used:{}'.format((end_time-start_time)*1000))

#keeps the thread active
while True:
    try:
        ans=input()
        if int(ans)==1:
            #ask for temperature nb
            data_to_send = {"action": 1, 'data':'0'}
            json_format = json.dumps(data_to_send)
            publish(json_format, 'nb/request')

        elif int(ans)==2:
            #start demand response nb
            data_to_send = {"action": 2, "data":'1'}
            json_format = json.dumps(data_to_send)
            publish(json_format, 'nb/request')

        elif int(ans)==3:
            #end demand response nb
            data_to_send = {"action": 3, "data":'0'}
            json_format = json.dumps(data_to_send)
            publish(json_format, 'nb/request')

        elif int(ans)==4:
            #request metering data
            data_to_send = {"action": 4, "day": 0, "hour": 5}
            json_format = json.dumps(data_to_send)
            publish(json_format, 'nb/request')
        elif int(ans)==5:
            #ping the device
            ping_data.clear()
            dateTimeObj = time.perf_counter()
            data_to_send = {"action": 5, "data": '0'}
            json_format = json.dumps(data_to_send)
            publish(json_format, 'nb/request')
            print(dateTimeObj)
            ping_data.append(dateTimeObj)

        #lora under this line
        #--------------------------------------------------------
        elif int(ans)==6: #ping device
            lora_ping_holder.clear()
            dateTimeObj = time.perf_counter()
            sent_to_lora(id_lora_device, 1)
            lora_ping_holder.append(dateTimeObj)
        elif int(ans)==7: #metering
            sent_to_lora(id_lora_device, 2)
        elif int(ans)==8: #receive temperature
            sent_to_lora(id_lora_device, 3)
        elif int(ans)==9: #turn of device
            sent_to_lora(id_lora_device, 4)
        elif int(ans) == 10:  # start demand response lora device
            sent_to_lora(id_lora_device, 5)
        elif int(ans)==11: #end demand response lora device
            sent_to_lora(id_lora_device, 6)
        elif int(ans) == 12:  # metering with day
            send_to_lora_v2(id_lora_device, [20, 0, 1])
        elif int(ans) == 13:  # metering with day and hour
            send_to_lora_v2(id_lora_device, [21, 0, 1, 0])
        elif int(ans) == 17:  # metering with day and hour
            send_to_lora_v2(id_lora_device, [16])
        elif int(ans) == 100:
            topic = "lorawan/downlink"
            to_send = {"thingName": "60C5A8FFFE79945B", "bytes": "Aw=="}
            json_format = json.dumps(to_send)
            publish(json_format, topic)

        elif int(ans)==14:
            #request metering data
            data_to_send = {"action": 6, "day": 2}
            json_format = json.dumps(data_to_send)
            ping_data.clear()
            dateTimeObj = time.perf_counter()
            publish(json_format, 'nb/request')
            #ping_data.append(dateTimeObj)
        else:
            print('bad input')
    except:
        pass


disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()

#start= time.perf_counter()
#end = time.perf_counter()
#print(end-start)