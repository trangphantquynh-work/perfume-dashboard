# Perfume Dashboard

Facebook Ads Analytics Dashboard for Parfumelite, built with Cloudflare D1, Workers, and Pages.

## Features

- 📊 Real-time Facebook Ads performance tracking
- 📅 Custom date range selection with Flatpickr
- 🔄 Period-over-period comparison (previous period)
- 📈 Interactive charts with Chart.js
- 🎨 Premium UI/UX with glassmorphism design
- 🌐 Vietnamese language support

## Tech Stack

- **Database**: Cloudflare D1 (SQLite)
- **Backend**: Cloudflare Workers
- **Frontend**: Cloudflare Pages (Vanilla JS, TailwindCSS)
- **Charts**: Chart.js
- **Icons**: Phosphor Icons

## Deployment

### Prerequisites
- Cloudflare account
- Wrangler CLI installed (`npm install -g wrangler`)

### Database Setup
```bash
# Create D1 database
npx wrangler d1 create parfumelite-ads

# Run schema
npx wrangler d1 execute parfumelite-ads --file=schema.sql --remote

# Import data
python3 csv_to_d1.py --auto Auto.csv --demographics Age_Gender.csv --regions Region.csv
npx wrangler d1 execute parfumelite-ads --file=import_data.sql --remote
```

### Deploy Worker
```bash
npx wrangler deploy worker.js --name parfumelite-dashboard
```

### Deploy Pages
```bash
npx wrangler pages deploy . --project-name=parfumelite-dashboard
```

## Live Demo

- **Dashboard**: https://parfumelite-dashboard.pages.dev
- **API**: https://parfumelite-dashboard.duongthien408.workers.dev

## Project Structure

```
.
├── index.html          # Main dashboard UI
├── worker.js           # Cloudflare Worker API
├── schema.sql          # Database schema
├── csv_to_d1.py        # Data import script
├── wrangler.toml       # Cloudflare configuration
└── README.md
```

## License

MIT
