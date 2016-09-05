#include <ESP8266WiFi.h>
#include <PubSubClient.h>
#include <Wire.h>

// Nanopb libraries
#include "pb.h"
#include "pb_encode.h"
#include "protocol.pb.h"

// References:
// https://learn.sparkfun.com/tutorials/esp8266-thing-hookup-guide
// https://blog.noser.com/arduino-iot-with-mqtt-and-nanopb/
// mqtt_esp8266 example

// WiFi information
const char WiFiSSID[] = "NETGEAR27";
const char WiFiPSK[]  = "melodichippo473";
WiFiClient espClient;

// MQTT information
const char SERVER[]  = "192.168.0.11";
const int SERVERPORT = 1883;
#define CLIENT_ID "1"
PubSubClient client(espClient);

// I2C information
const int SENSOR_I2C_ADDR = 0x48;

// Pin definitions
const int LED_PIN = 5; // Thing's onboard, green LED

void setup()
{
  // Start serial for debug messages
  Serial.begin(9600);

  // Setup LED pin and connect wifi
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
  connectWiFi();
  digitalWrite(LED_PIN, HIGH); // Once we're done connecting, leave LED off

  // Setup MQTT client
  client.setServer(SERVER, SERVERPORT);

  // Start I2C on default pins (2 -> SDA, 14 -> SCL)
  Wire.begin(); 
}

void connectWiFi()
{
  byte ledStatus = LOW;

  // Set WiFi mode to station
  WiFi.mode(WIFI_STA);

  // Start the WiFi connection
  WiFi.begin(WiFiSSID, WiFiPSK);

  // Waut for the WiFi client to connect, blinking the LED during that time
  while (WiFi.status() != WL_CONNECTED)
  {
    digitalWrite(LED_PIN, ledStatus); // Write LED high/low
    ledStatus = (ledStatus == HIGH) ? LOW : HIGH;

    delay(100);
  }
}

void loop()
{
  // If the wifi disconnects, we need to reconnect
  while (WiFi.status() != WL_CONNECTED)
  {
    connectWiFi();
    delay(100);
  }

  // Connect for the first time or reconnect
  mqttConnect();

  // Create byte buffer for sending out on MQTT
  uint8_t sMsg[MQTT_MAX_PACKET_SIZE];

  // Create message struct
  TemperatureMessage message;
  message.clientId = 1;
  message.temperatureValue = getTemperature();

  // Prepare to encode nessage
  pb_ostream_t buffer = pb_ostream_from_buffer(sMsg, sizeof(sMsg));

  // Try to encode
  if (!pb_encode(&buffer, TemperatureMessage_fields, &message)) {
    Serial.println(F("Encoding failed"));
    Serial.println(PB_GET_ERROR(&buffer));
  }
  else
  {
    // If successful, send the message out
    client.publish("temp/"CLIENT_ID, sMsg, buffer.bytes_written);
  }

  delay(200); 
}

// Connect and reconnect as necessary to the MQTT server.
void mqttConnect()
{
  while (!client.connected())
  {
    Serial.print("Attempting MQTT connection...");
    
    // Attempt to connect
    if (client.connect("ESP8266Client"))
    {
      Serial.println(" Connected.");
    }
    else
    {
      Serial.print(" Failed, rc=");
      Serial.print(client.state());
      Serial.println(" try again in 2 second.s");
      
      // Wait 2 seconds before retrying
      delay(2000);
    }
  }
}

int getTemperature()
{
  Wire.requestFrom(SENSOR_I2C_ADDR, 2); 

  byte MSB = Wire.read();
  byte LSB = Wire.read();

  int TemperatureSum = ((MSB << 8) | LSB) >> 4; 

  return TemperatureSum;
}
