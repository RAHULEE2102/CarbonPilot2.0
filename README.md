# ⬡ CarbonPilot

**Real-Time Fleet CO₂ Intelligence Platform — No Fuel Sensor Required**

> Built on **LBCA-Net** (Leakage-Free Behavior-Aware Cross-class Adaptive Network), validated on 662,249 real OBD-II + SAE J1939 records across 5 vehicles in Brazil.
> 
> *Published: IEEE Transactions on Intelligent Transportation Systems (T-ITS)*  
> *Author: Rahul Kumar Dubey, Senior Member IEEE — Bosch Global Software Technologies, Bengaluru*

---

## 🏁 Product Name

**CarbonPilot** — because it *pilots* your fleet's carbon compliance with precision, intelligence, and zero sensor lock-in.

---

## 🚀 Live Demo

**Render deployment:** `https://carbonpilot.onrender.com`

---

## 🎯 What CarbonPilot Solves

| Problem | Existing Market | CarbonPilot |
|---------|----------------|-------------|
| Fuel-flow sensor required | 78% of CO₂ models need fuel PID | ✅ Zero fuel-flow dependency |
| Protocol fragmentation | OBD-II and J1939 never bridged | ✅ Single model, both protocols |
| Idle period ignored | All models predict g/km only | ✅ Dual-regime: idle g/min + motion g/km |
| Physics violations | Unconstrained neural predictions | ✅ Willans-line penalty |
| Batch-only reporting | COPERT-5, MOVES3: no real-time | ✅ 1-Hz, 3.1ms inference |
| CSRD compliance gap | Manual calculation required | ✅ ISO 14064 + CSRD output |

---

## 📊 Validated Performance

```
Car-2  (Hyundai HB20, OBD-II):  RMSE = 24.3 ± 2.1 g/km   R² = 0.921
Truck-3 (FIL 2471, SAE J1939): RMSE = 118.6 ± 9.4 g/km  R² = 0.876
Zero-shot car→truck transfer:   172.8 g/km vs 248.7 g/km (−30.5%)
Inference latency:               3.1 ms / 64-second window (CPU)
```

---

## 🏗️ Architecture

```
OBD-II (6 signals)  ──► Encoder E_c  ──┐
                                        ├──► MMD Alignment h^s ∈ ℝ⁶⁴
J1939  (14 signals) ──► Encoder E_k  ──┘         │
                                        Behavior β (W=64s)
                                                  │
                                        Concat h̃ = [h^s; β]
                                                  │
                                        3-Layer LSTM [128→256→128]
                                                  │
                                    ┌─────────────┼─────────────┐
                                Regime ẑₜ   Motion ŷ^(m)   Idle ŷ^(i)
                                              g/km          g/min
                                                  │
                                        Gated Output ŷₜ
                                        + Willans Physics Penalty
```

---

## 📁 Repository Structure

```
carbonpilot/
├── main.py              # FastAPI backend + streaming demo API
├── templates/
│   └── index.html       # Full product page (hero, architecture, live POC)
├── static/              # Static assets
├── requirements.txt     # Python dependencies
├── render.yaml          # Render.com deployment config
├── Procfile             # Process definition
└── README.md
```

---

## 🛠️ Local Development

```bash
# Clone
git clone https://github.com/your-org/carbonpilot.git
cd carbonpilot

# Install
pip install -r requirements.txt

# Run
uvicorn main:app --reload --port 8000

# Open
open http://localhost:8000
```

---

## ☁️ Deploy to Render (Free Tier)

1. Fork this repository
2. Go to [render.com](https://render.com) → New → Web Service
3. Connect your GitHub repository
4. Render auto-detects `render.yaml` — click **Deploy**
5. Your URL: `https://carbonpilot-xxxx.onrender.com`

> **Free tier note:** App sleeps after 15 min inactivity. First request takes ~30s to wake.

---

## 📦 API Endpoints

```
GET /                          Product page
GET /api/stream/{vehicle_id}   Live telemetry (1 data point)
GET /api/trip/{vehicle_id}     120-second trip history
GET /api/vehicles              Vehicle profiles + paper statistics
GET /api/metrics               Aggregate performance metrics
```

**Vehicle IDs:** `car1` | `car2` | `truck1` | `truck2` | `truck3`

---

## 📈 Dataset

| Vehicle | Protocol | Records | CO₂ Median | Idle % |
|---------|----------|---------|------------|--------|
| Car-1 · Renault Sandero | OBD-II | 85,095 | 151 g/km | 9.5% |
| Car-2 · Hyundai HB20 | OBD-II | 6,699 | 335 g/km | 14.8% |
| Truck-1 · EVO 8726 | SAE J1939 | 159,406 | 1,302 g/km | 10.8% |
| Truck-2 · FKQ 5624 | SAE J1939 | 191,563 | 1,357 g/km | 26.2% |
| Truck-3 · FIL 2471 | SAE J1939 | 219,486 | 1,173 g/km | **35.2%** |

**Total:** 662,249 records · Belo Horizonte & São Paulo, Brazil · 2014–2021

---

## 📄 License

MIT License — Research implementation. For production fleet deployment, contact the author.

---

## 📬 Contact

**Rahul Kumar Dubey** · Senior Member, IEEE  
Bosch Global Software Technologies Pvt. Ltd., Bengaluru, India  
RahulKumar.Dubey@in.bosch.com
