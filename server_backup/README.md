# Server-Backup

Sichert einen **kompletten Linux-Server** (alle Dateien, Einstellungen, installierte Programme,
Datenbanken, Docker-Container, Benutzer, SSH-Host-Keys …) per SSH auf diesen Windows-PC – so dass
man den Server komplett neu aufsetzen („nuken“) und danach das Backup einfach zurückspielen kann.

Bedienung im Browser: **http://127.0.0.1:5042**

## Start

1. Python 3.9+ installieren (https://www.python.org/downloads/, „Add python.exe to PATH“ anhaken).
2. **`start.bat`** doppelklicken. Beim ersten Start wird eine eigene Python-Umgebung angelegt und
   `paramiko` installiert, danach öffnet sich die Oberfläche im Browser.
3. Das Fenster offen lassen – solange das Programm läuft, laufen auch die geplanten Backups.
   Mit **„Mit Windows starten“** (Karte *Zeitplan*) startet es automatisch bei der Anmeldung.

## Einstellungen in der Oberfläche

| Bereich | Was man einstellt |
|---|---|
| **Server** | Domain oder IP, SSH-Port, Benutzer, Passwort, optional SSH-Schlüssel. „Verbindung testen“ prüft Anmeldung, sudo-Rechte und tar. |
| **Zeitplan** | Nur manuell / alle X Stunden / täglich / wöchentlich / monatlich + Uhrzeit. Verpasste Termine werden nachgeholt, Fehlschläge bis zu 3× wiederholt. |
| **Speicherort & Aufbewahrung** | Zielordner (auch Netzlaufwerk/USB-Platte), wie viele Backups behalten werden und ab welchem Alter gelöscht wird. Einzelne Backups können „geschützt“ werden. |
| **Art der Sicherung** | Komplettes Dateisystem (empfohlen) oder Festplatten-Image; zusätzliche Ausschlüsse; Befehle vor/nach dem Backup (z. B. Datenbank-Dump). |
| **Vorhandene Backups** | Liste mit Größe/Status, Wiederherstellen, Löschen, Schützen. |

Das Passwort wird mit Windows-DPAPI verschlüsselt in `backup_config.json` gespeichert (nur das
eigene Windows-Konto kann es entschlüsseln) und nie an den Browser zurückgegeben. Ist der Benutzer
nicht `root`, wird dasselbe Passwort für `sudo` verwendet – ein komplettes Backup braucht Root-Rechte.

## Was passiert beim Backup?

* **Komplettes System:** Auf dem Server läuft `tar` über das ganze Dateisystem (mit Rechten,
  Besitzern, ACLs, xattrs) und wird komprimiert direkt auf den PC gestreamt – auf dem Server wird
  kein zusätzlicher Platz gebraucht. Ausgelassen werden nur Dinge, die das System beim Start neu
  erzeugt (`/proc`, `/sys`, `/dev`, `/run`, `/tmp`, Swap, Paket-Cache).
* **Festplatten-Image:** `dd` der ganzen Festplatte, gzip-komprimiert. Bit-genau, aber so groß wie
  die Festplatte belegt ist und nur aus dem **Rescue-System** des Anbieters wirklich konsistent.
* Die Datei wird erst als `.partial` geschrieben, auf Vollständigkeit geprüft und dann umbenannt.
  Erst **nach** einem erfolgreichen Backup werden alte Backups nach den Regeln gelöscht.
* Neben jedem Backup liegt eine `.json` mit Prüfsumme und Serverinfos (Betriebssystem,
  Partitionen, IP-Adressen) – hilfreich beim Neuaufsetzen.

Tipp für Datenbanken: unter „Befehl vor dem Backup“ einen Dump anlegen, z. B.
`mysqldump --all-databases > /root/db.sql` oder `docker exec db pg_dumpall -U postgres > /root/pg.sql`.

## Server nuken und wiederherstellen

**Komplett-Backup:**
1. Server beim Anbieter mit **demselben Betriebssystem/Version** neu installieren
   (z. B. Ubuntu 22.04 → wieder Ubuntu 22.04).
2. In der Oberfläche Server/Benutzer/Passwort des **frischen** Systems eintragen
   (meist `root` + neues Passwort) und **„Host-Key zurücksetzen“** klicken.
3. Beim gewünschten Backup **„Wiederherstellen“**, Servernamen zur Bestätigung eintippen.
4. Das Backup wird eingespielt, danach werden initramfs und Bootloader für die neue Festplatte
   eingerichtet und der Server startet neu. Danach läuft alles wie zum Zeitpunkt des Backups –
   auch die alten Benutzer/Passwörter und SSH-Host-Keys (ggf. noch einmal „Host-Key zurücksetzen“).

**Festplatten-Image:** Server im Rescue-System starten, dessen Zugangsdaten eintragen, unter
„Festplatte“ das Ziel eintragen (z. B. `/dev/sda`), dann „Wiederherstellen“.

## Fehler „Permission denied (publickey,password)“

Bedeutet: Benutzer oder Passwort falsch, bzw. es war kein Passwort hinterlegt und der angegebene
SSH-Schlüssel existierte nicht. In der Oberfläche das Passwort eintragen, speichern und
„Verbindung testen“ klicken. Erlaubt der Server keine Passwort-Anmeldung
(`PasswordAuthentication no`), den Pfad zum privaten Schlüssel eintragen.

## Dateien

| Datei | Zweck |
|---|---|
| `start.bat` | Starter für Windows |
| `backup_app.py` | Programm (Webserver, Zeitplan, Backup/Restore) |
| `backup_ui.html` | Oberfläche |
| `backup_config.json`, `backup_state.json`, `backup_log.txt`, `known_hosts` | werden beim Start angelegt (nicht im Repository) |

Die Oberfläche ist nur vom eigenen PC aus erreichbar (127.0.0.1). Port ändern: `start.bat --port 5050`.
