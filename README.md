# DiVA_kolleKTHor-WoS


***

## 1. Kort beskrivning

**Detta program läser en KTH DiVA‑export (CSV) för ett valt år/intervall, hittar publikationer som saknar ISI‑ID (Web of Science UID), och försöker fylla på dessa genom att fråga Web of Science Starter API i två steg.**

Skriptet är tänkt som ett **syskon** till Crossref‑skriptet i **DiVA_kolleKTHor**‑projektet:

- Samma grundstruktur och kodlayout.
- Liknande kolumnnamn och utdataformat.
- Både **CSV** och **Excel** med klickbara länkar till DiVA och Web of Science.

***

## 2. Huvudfunktioner

**Tvåstegad WoS‑uppslagning för saknade ISI‑ID:**

1. **Runda 1 – DOI → WOS UID**
    - Filtrerar fram poster som:
        - ligger inom angivet år/intervall,
        - har en **DOI**,
        - saknar **ISI‑ID/WOS UID**.
    - Frågar **Web of Science Starter API** med söksträngen `DO=<doi>` och hämtar dokumentets **uid** (WOS UID) om träff finns.
    - Matchade poster får ett **verifierat WOS UID**.
2. **Runda 2 – Titel + år → WOS UID**
    - Körs endast på de poster som fortfarande saknar ISI/WOS UID efter runda 1.
    - Frågar WoS med kombinationer av t.ex. **titel (TI)** och **publiceringsår (PY)**.
    - Använder liknande typ av **verifieringslogik** som Crossref‑skriptet:
        - titel‑likhet,
        - publikationstyp,
        - ISSN/ISBN, volym, nummer, sidor,
        - författar‑efternamn (när tillgängligt).
    - Ger antingen **Verified_WOS_UID** eller **Possible_WOS_UID** beroende på hur stark matchningen är.

**Utdata:**

- En **CSV‑fil** med originaldata från DiVA plus nya kolumner för bl.a.:
    - `Verified_WOS_UID`
    - `Possible_WOS_UID`
    - eventuella kommentars-/statusfält (t.ex. hur matchen hittades)
- En **Excel‑fil** med:
    - samma kolumner som CSV,
    - automatiskt genererade **hyperlänkar** till:
        - **DiVA‑posten** för varje rad,
        - motsvarande **Web of Science‑post** när WOS UID hittats.

***

## 3. Typiskt arbetsflöde

1. **Exportera data från KTH DiVA**
    - Gör en **CSV‑export** för önskat år eller årsspann.
    - Se till att kolumner som **DOI, ISI, ScopusId, PMID, titel, år, publikationstyp, ISSN/ISBN, volym, nummer, sidor, författare** följer den struktur som både Crossref‑ och WoS‑skriptet förväntar sig.
2. **Kör WoS‑skriptet mot CSV‑filen**
    - Ange:
        - **in‑fil** (DiVA‑CSV),
        - **ut‑filnamn** för CSV/Excel,
        - **år eller årsspann**,
        - **WoS API‑nyckel**,
        - eventuellt begränsningar/rate‑delay beroende på konfiguration.
3. **Granska resultatet**
    - Öppna **Excel‑filen**.
    - Klicka igenom **DiVA‑länkarna** och **WoS‑länkarna** för att manuellt granska gränsfall.
    - Använd kolumner som `Verified_WOS_UID` respektive `Possible_WOS_UID` för att se var matchningen är stark respektive osäker.

***

## 4. Förväntad målgrupp och användningsfall

Detta skript är riktat till:

- **Bibliometriker**, bibliotekarier och **forskningsadministratörer** vid KTH (eller andra DiVA‑anslutna lärosäten) som vill:
    - komplettera DiVA‑poster med **saknade ISI‑ID**,
    - förbereda data för **citeringsanalys** och andra bibliometriska studier,
    - få en reproducerbar, skriptbaserad process parallell med Crossref‑baserade DOI‑kompletteringar.

Programmets design gör det lämpligt att:

- köras **årsvis** eller för valda spann av år,
- integreras i en återkommande **datakvalitetsrutin**,
- jämföras sida vid sida med Crossref‑skriptet i samma **DiVA_kolleKTHor**‑miljö.

***

## 5. Installation och beroenden (översikt)

Skriptet är implementerat i **Python 3** och använder vanliga paket för datahantering och HTTP‑anrop.

Typiska beroenden:

- **pandas** – CSV‑/Excel‑hantering
- **requests** – API‑anrop mot Web of Science Starter API
- **tqdm** – progressbar i terminalen
- **datetime** – paket för datum/tid
- **xlsxwriter** – paket för Excel‑skrivning

***

## 6. Konfiguration

De viktigaste inställningarna ligger i toppen av skriptet:

- **Input‑fil:** sökväg till DiVA‑CSV
- **Output‑filer:** basnamn för CSV + Excel
- **År / årsspann:** filtrerar vilka DiVA‑poster som behandlas
- **WoS API‑nyckel:** `X-ApiKey` för Web of Science Starter API
- **Rate limiting:** eventuell paus mellan anrop för att vara snäll mot API:et

***

## 7. Relation till syskonskriptet (Crossref)

Detta WoS‑skript är tänkt att:

- komplettera Crossref‑skriptet i **DiVA_kolleKTHor**,
- dela samma **struktur, kolumnupplägg och filosofier för matchningslogik**,
- möjliggöra en **sammanhängande kedja**:

1. Crossref‑skriptet: hitta/förbättra **DOI** för poster utan externa ID.
2. WoS‑skriptet: utifrån DOI (och vid behov titel/år) fylla i **saknade ISI‑ID/WOS UID**.

Det gör det lätt att:

- diff:a och förstå båda skripten sida‑vid‑sida,
- återanvända erfarenheter från Crossref‑matchningen,
- dokumentera samma dataflöde i README och intern dokumentation.

***

## 8. License

This project is licensed under the MIT License.

Copyright (c) 2025 Anders Wändahl

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. 

***

