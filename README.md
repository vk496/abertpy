# abertpy

## Overview

**abertpy** is a utility designed to enable Spanish TDT (Digital Terrestrial Television) reception via the Hispasat 30.0W satellite using TVHeadend. Unlike other tools that depend on proprietary hardware or closed setups, abertpy offers a more flexible and automated integration with TVHeadend.

### Key Features

- 🔄 Automatically recreates MUXes based on live reception (no static lists)
- 📅 EPG (Electronic Program Guide) support
- 🌐 Supports both local and remote TVHeadend setups (local is recommended)
- 🧩 Fully integrated. No external scripts required

---

## Prerequisites

Before installing abertpy, ensure the following components are properly set up:

- ✅ A working TVHeadend instance already receiving Abertis channels via Hispasat 30.0W
- 🧪 [TSDuck](https://tsduck.io/) installed (specifically the `tsanalyze` binary)
- 🔐 [Oscam-emu](https://hub.docker.com/r/chris230291/oscam-emu) running and configured in TVHeadend  
  - ⚠️ Use **CCcam** protocol instead of **DVB-API**
- 📦 [pipx](https://github.com/pypa/pipx) installed (optional but recommended)

---

## Installation

1. **Install abertpy globally via pipx:**
   ```bash
   sudo pipx install --global abertpy
   ```

2. **Verify TVHeadend can access abertpy:**
   ```bash
   sudo su hts -s /bin/bash -c "abertpy ping"
   ```

3. **Discover your Hispasat network UUID (this will fail intentionally to list available networks):**
   ```bash
   abertpy -t http://tvheadend.lan:9981/
   ```

4. **Run the setup with the correct network UUID:**
   ```bash
   abertpy -t http://tvheadend.lan:9981/ -n <your_network_uuid>
   ```

5. **(Optional) Add or update your SoftCam.key file:**
   [SoftCam.key gist](https://gist.github.com/vk496/c524292b974837b4a17fe7264f412284)
