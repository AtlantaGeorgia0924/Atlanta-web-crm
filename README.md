# Atlanta Stock Web

A comprehensive stock management and sales tracking system with real-time inventory sync across Google Sheets, built with React + Vite frontend and FastAPI backend.

## Features

- **Stock Management**: Browse, filter, and manage product inventory with real-time status tracking
- **Sales Cart**: Add items to cart, set buyer details, and checkout with immediate sheet sync
- **Inventory Tracking**: Track pending deals, mark payments (PAID, PART PAYMENT, UNPAID), and update fulfillment status
- **Service Records**: Create non-stock service entries directly in inventory
- **Debtors & Billing**: Generate bills, track outstanding amounts, send WhatsApp notifications
- **Google Sheets Integration**: Queue-first writes with automatic sync to stock and inventory sheets
- **Multi-sheet Support**: Sync across multiple Google Sheets for stock, inventory, and client data
- **Real-time Updates**: Immediate sheet replay for payment and status changes

## Project Structure

```
├── frontend/                 # React + Vite SPA
│   ├── src/
│   │   ├── WorkspaceApp.jsx # Main workspace component
│   │   ├── workspace.css    # Styling
│   │   └── api/             # API call wrappers
│   ├── package.json
│   └── vite.config.js
├── backend/                  # FastAPI runtime
│   ├── main.py             # FastAPI app setup
│   ├── runtime.py          # Write orchestration & queue
│   ├── routers/            # API endpoints
│   └── dependencies.py     # DI setup
├── services/               # Business logic
│   ├── stock_service.py
│   ├── billing_service.py
│   ├── client_service.py
│   └── sync_service.py
├── Main.py                 # GUI entry point
├── run_api.py             # Backend server entry point
└── run.py                 # Frontend server entry point
```

## Setup

### Prerequisites
- Python 3.10+
- Node.js 16+
- Google Sheets API credentials
- PostgreSQL (optional, for queue persistence)

### Backend Setup

1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure credentials:
   - Place `credentials.json` (Google Sheets API) in project root
   - Update `config.json` with your sheet IDs

4. Start the backend:
   ```bash
   python3 run_api.py
   ```

### Frontend Setup

1. Install dependencies:
   ```bash
   cd frontend
   npm install
   ```

2. Configure API base URL (optional):
   ```bash
   export VITE_API_BASE_URL=http://127.0.0.1:8000
   ```

3. Start development server:
   ```bash
   npm run dev
   ```

4. Build for production:
   ```bash
   npm run build
   ```

## Environment Variables

### Frontend (.env.local)
```
VITE_API_BASE_URL=http://127.0.0.1:8000
```

### Backend
```
POSTGRES_DSN=postgresql://user:password@localhost/atlanta_stock
```

## API Endpoints

- `GET /health` - Health check
- `GET /api/stock/view/live` - Get live stock view
- `POST /api/stock/live/add` - Add new stock item
- `POST /api/stock/live/pending/payment` - Update pending deal payment status
- `POST /api/stock/live/cart/checkout` - Checkout cart
- `GET /api/workspace/debtors` - Get debtors list
- `POST /api/billing/send-whatsapp` - Send WhatsApp message

## Usage

1. **Browse Products**: Navigate to Products view to see all stock items filtered by status
2. **Add to Cart**: Click "Add to Cart" on any product
3. **Manage Cart**: Update buyer details, payment status, and availability
4. **Checkout**: Click "Sell Out Cart" to sync sales to both sheets
5. **Pending Deals**: Use floating Pending Deals button to update payment fulfillment
6. **Services**: Add non-stock services via floating Add Service button
7. **Billing**: Generate and send bills to debtors via WhatsApp

## Architecture

- **React + Vite**: Fast, modern frontend with hot module replacement
- **FastAPI**: High-performance backend with automatic Swagger docs
- **Google Sheets**: Source-of-truth external data store
- **PostgreSQL Queue**: Optional persistence layer for reliable writes
- **Queue-First Pattern**: All writes queue immediately, replay to Sheets asynchronously

## License

Proprietary

## Contact

Atlanta Georgia Tech Team
