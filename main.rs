use image::{GrayImage, Luma};
use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
use std::time::Duration;

fn epd_to_png(raw: &[u8], width: u32, height: u32) -> GrayImage {
    let mut img = GrayImage::new(width, height);
    for y in 0..height {
        for x in 0..width {
            let idx = (y * width + x) as usize;
            let byte = raw[idx / 8];
            let bit = (byte >> (7 - (idx % 8))) & 1;
            let pixel = if bit == 1 { 255u8 } else { 0u8 };
            img.put_pixel(x, y, Luma([pixel]));
        }
    }
    img
}

#[tokio::main]
async fn main() {
    let mut mqttoptions = MqttOptions::new("1000", "192.168.103.200", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(10));
    mqttoptions.set_max_packet_size(65536, 65536);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    client.subscribe("1000/image", QoS::AtLeastOnce).await.unwrap();
    client.publish("1000/awake", QoS::AtLeastOnce, false, "awake,100,0x00").await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(msg))) => {
                    if msg.topic == "1000/image" {
                        println!("📨 Bild empfangen: {} Bytes", msg.payload.len());
                        return Some(msg.payload.to_vec());
                    }
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => println!("✅ Subscribed"),
                Ok(Event::Incoming(Packet::PubAck(_))) => println!("✅ Publish bestätigt"),
                Ok(Event::Outgoing(Outgoing::Disconnect)) => return None,
                Ok(_) => {}
                Err(e) => {
                    eprintln!("❌ Fehler: {:?}", e);
                    return None;
                }
            }
        }
    })
        .await;

    match result {
        Ok(Some(bytes)) => {
            println!("✅ {} Bytes empfangen", bytes.len());
            let img = epd_to_png(&bytes, 400, 300);
            match img.save("empfangen.png") {
                Ok(_) => println!("💾 Gespeichert als empfangen.png"),
                Err(e) => eprintln!("❌ Speichern fehlgeschlagen: {}", e),
            }
        }
        Ok(None) => eprintln!("❌ Verbindung verloren"),
        Err(_) => eprintln!("⏰ Timeout"),
    }

    client.disconnect().await.ok();
}
