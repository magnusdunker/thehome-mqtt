# MQTT til TheHome

> **Udgået.** TheHome bruger sin indbyggede MQTT-integration, som kan forbinde til flere brokere, også HiveMQ Cloud. Dette repo opdateres ikke længere.

En ekstern integration til [TheHome](https://github.com/magnusdunker/thehome), der forbinder til en MQTT-broker og opretter enheder ud fra Home Assistants MQTT discovery. Zigbee2MQTT, Z-Wave JS UI, Tasmota, ESPHome (MQTT), Frigate og OpenMQTTGateway dukker derfor op af sig selv.

## Installation

I TheHome: **Indstillinger → Integrationer → Fra GitHub**. Indsæt

```
https://github.com/magnusdunker/thehome-mqtt
```

og tryk **Installér**. Tilføj derefter **MQTT** under **Tilføj integration**. TheHome foreslår den også, når den finder en broker på netværket via mDNS.

## Opsætning

- **Broker og port**: værtsnavn eller IP-adresse. Lader du porten stå tom, bruges 1883, eller 8883 med TLS.
- **Brugernavn og adgangskode**, hvis brokeren kræver det.
- **TLS**: Automatisk slår TLS til på port 8883 og for brokere uden for dit netværk. Til og Fra tvinger det.
- **Start indbygget broker**: kører en broker i integrationen på port 1883 i stedet for at forbinde til en anden.

Under avancerede indstillinger:

- **CA-certifikat (PEM)** til en broker med sit eget certifikat. Uden det tjekkes certifikatet mod de almindelige udstedere, som cloud-brokere bruger.
- **Spring certifikattjek over** til en lokal broker med selvsigneret certifikat. Forbindelsen er stadig krypteret, men ikke beskyttet mod en mellemmand.
- **Klientcertifikat og -nøgle (PEM)**, hvis brokeren kræver dem.
- **MQTT over WebSocket** med en sti, f.eks. `/mqtt`.
- **Keep-alive** og **klient-id**.

Integrationen prøver at forbinde, før opsætningen gemmes, og fortæller, hvis brugernavn eller adgangskode er forkert, hvis certifikatet ikke er betroet eller ikke passer til værtsnavnet, eller hvis TLS-forbindelsen ikke kan oprettes.

### HiveMQ Cloud og andre cloud-brokere

Indsæt værtsnavnet, f.eks. `xxxx.s2.eu.hivemq.cloud`, samt brugernavn og adgangskode fra HiveMQ's konsol. TLS og port 8883 vælges af sig selv, fordi brokeren ikke er på dit netværk. WebSocket over TLS på port 8884 med stien `/mqtt` virker også.

### Indstillinger

Efter opsætningen kan du under integrationens indstillinger slå discovery til og fra, ændre discovery-præfikset (standard `homeassistant`), slå online/offline-status på `<præfiks>/status` fra og vælge **topics at abonnere på**. Beskeder på dem sendes som hændelsen `mqtt_message`, som automationer med en MQTT-trigger lytter efter.

## Hvad du får

Discovery opretter enheder og entities af typerne sensor, binary_sensor, switch, light (også JSON- og template-skema), climate, cover, fan, lock, number, select, button, text, siren, valve, update, event og scene, samt enhedstriggere (device_automation), med tilgængelighed, `expire_after`, `off_delay`, skabeloner og JSON-attributter som i Home Assistant. Discovery pr. enhed (`<præfiks>/device/.../config`) virker også.

Tjenester under domænet `mqtt_ext`:

| Tjeneste | |
|---|---|
| `publish` | send en besked (topic, payload, payload_template, qos, retain) |
| `subscribe`, `unsubscribe` | lyt på et topic indtil næste genindlæsning; beskeder sendes som `mqtt_message` |
| `debug_info` | forbindelse, abonnementer, entities og de seneste beskeder |
| `zigbee2mqtt_bridges` | de Zigbee2MQTT-broer, integrationen har set |
| `zigbee2mqtt_enable_discovery` | slå Home Assistant-integrationen til i Zigbee2MQTT med TheHomes præfiks |
| `zigbee2mqtt_permit_join` | tillad nye Zigbee-enheder |
| `zigbee2mqtt_rename_device`, `zigbee2mqtt_remove_device` | omdøb eller fjern en Zigbee2MQTT-enhed |

## Forholdet til den indbyggede integration

TheHome har også MQTT indbygget (`mqtt`). Den eksterne integration har sit eget domæne, `mqtt_ext`, og sine egne enheds-id'er, så de to kan være installeret samtidig uden at blande enhederne sammen. Bruger de samme broker og samme discovery-præfiks, får du hver enhed to gange, så brug kun den ene ad gangen. Koden er porteret fra den indbyggede, men nogle ting kan en ekstern integration ikke:

- Når en enhed fjerner sig selv med en tom discovery-besked, bliver dens entities utilgængelige i stedet for at blive slettet. Du kan slette dem i TheHome.
- Automationer med en MQTT-trigger får ikke selv integrationen til at abonnere på deres topic. Tilføj topic'et under **Abonnér på topics** i integrationens indstillinger.
- Knappen **Slå til i Zigbee2MQTT** under Reparationer virker kun med den indbyggede. Brug tjenesten `mqtt_ext.zigbee2mqtt_enable_discovery` i stedet.
- Andre indbyggede integrationer, der finder deres enheder via MQTT, bruger kun den indbyggede MQTT.

## Udvikling

Bygget i Rust på SDK'en `thehome-plugin` fra [thehome-example-integration-rust](https://github.com/magnusdunker/thehome-example-integration-rust), med rumqttc som klient, rumqttd som indbygget broker og rustls til TLS.

- `src/manager.rs`, `src/entity.rs`, `src/discovery.rs`: MQTT-kernen, som i den indbyggede integration
- `src/hub.rs`, `src/host.rs`: kernens forbindelse til TheHome over plugin-protokollen
- `src/client.rs`: forbindelsen til brokeren, TLS og fejlbeskeder
- `src/lib.rs`: opsætning, indstillinger og tjenester
- `tests/`: kernen mod en falsk TheHome, og plugin'et mod den indbyggede broker, en TLS-broker med selvsigneret CA og en broker, der afviser login

```
cargo test
cargo test -- --ignored hivemq   # forbinder til HiveMQ Cloud med opdigtede loginoplysninger
cargo build --release
```

En ny version udgives med et tag, f.eks. `git tag v0.2.0 && git push origin v0.2.0`. Workflowet bygger til Windows, Linux (x86_64 og arm64, statisk musl) og macOS, lægger binærerne på releasen og skriver releaseteksten ud fra commits siden sidste tag, hvis releasen ikke har en.

## Licens

MIT
