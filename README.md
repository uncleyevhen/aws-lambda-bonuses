# AWS Lambda Bonus System

Comprehensive AWS Lambda-based bonus and promo code management system with automatic replenishment and frontend integration.

## 🎯 System Overview

This project implements a complete bonus point and promo code system using AWS Lambda functions, S3 storage, and browser automation for seamless e-commerce integration.

### Core Components

- **Bonus Accrual Lambda** - Handles bonus point calculations and customer rewards
- **Get Promo Code Lambda** - API endpoint for retrieving promo codes from S3
- **Replenish Promo Code Lambda** - Automated system for code replenishment from BON admin
- **KeyCRM Proxy Lambda** - Integration service for KeyCRM API
- **GTM Frontend Scripts** - Real-time bonus notifications and checkout integration

## 🚀 Features

### Automated Promo Code Management
- **Auto-replenishment**: Triggers when codes drop below threshold (3 codes)
- **Browser automation**: Playwright-based BON admin scraping
- **S3 storage**: Reliable cloud storage for available and used codes
- **Multiple amounts**: Support for 100, 200, 300 UAH promo codes

### Real-time Bonus System
- **10% bonus accrual** on order completion
- **Bonus usage tracking** for discounts
- **Customer notifications** with animated UI
- **Mobile-responsive** design

### Robust Architecture
- **Containerized deployment** using Docker and ECR
- **CloudWatch monitoring** and logging
- **Error handling** and retry mechanisms
- **Comprehensive testing** suite

## 📁 Project Structure

```
aws_lambda_bonuses/
├── bonus_accrual_lambda/         # Bonus points calculation
├── get_promo_code_lambda/        # Promo code API endpoint
├── replenish_promo_code_lambda/  # Automated code replenishment
├── keycrm_proxy_lambda/          # KeyCRM integration
├── gtm_integration/              # Frontend GTM scripts
├── promo_generator/              # Bulk promo code tools
└── batch_config.env             # Environment configuration
```

## 🛠 Setup & Deployment

### Prerequisites
- AWS CLI configured
- Docker installed
- Python 3.11+
- Node.js (for frontend testing)

### Quick Start

1. **Deploy Lambda Functions**
   ```bash
   # Deploy bonus accrual
   cd bonus_accrual_lambda
   ./scripts/deploy.sh

   # Deploy promo code API
   cd ../get_promo_code_lambda
   ./scripts/deploy.sh

   # Deploy replenish system
   cd ../replenish_promo_code_lambda
   ./scripts/deploy.sh
   ```

2. **Configure S3 Bucket**
   ```bash
   aws s3 mb s3://lambda-promo-sessions
   aws s3 cp get_promo_code_lambda/initial_codes.json s3://lambda-promo-sessions/promo-codes/available_codes.json
   ```

3. **Setup API Gateway**
   ```bash
   cd get_promo_code_lambda
   ./scripts/create_api_gateway.sh
   ```

## 🔧 Configuration

### Environment Variables
```bash
# BON Admin Credentials
BON_LOGIN=your_admin_login
BON_PASSWORD=your_admin_password

# S3 Configuration
S3_BUCKET=lambda-promo-sessions
S3_PREFIX=promo-codes/

# API Endpoints
BONUS_API_URL=https://your-api-gateway-url/prod/bonus-accrual
PROMO_API_URL=https://your-api-gateway-url/get-code
```

### Frontend Integration
Add GTM scripts to your checkout pages:
```html
<!-- Thank You Page -->
<script src="gtm_integration/bonus_thankspage_script.html"></script>

<!-- Checkout Page -->
<script src="gtm_integration/bonus_checkoutpage_script.html"></script>
```

## 📊 API Endpoints

### Get Promo Code
```http
GET https://k3pok2o5t1.execute-api.eu-north-1.amazonaws.com/get-code?amount=100
```

Response:
```json
{
  "success": true,
  "promo_code": "BON100X5UN",
  "amount": 100
}
```

### Bonus Accrual
```http
POST https://duu98ifeda.execute-api.eu-north-1.amazonaws.com/prod/bonus-accrual
```

Body:
```json
{
  "orderId": "12345",
  "orderTotal": 1000,
  "customer": {
    "name": "John Doe",
    "phone": "+380123456789",
    "email": "john@example.com"
  },
  "bonusAmount": 100,
  "usedBonusAmount": 50
}
```

## 🧪 Testing

### Run Test Suite
```bash
cd get_promo_code_lambda
./test_suite.sh
```

### Manual Testing
```bash
# Test API endpoint
python test_api_quick.py

# Check S3 state
python check_s3_state.py

# Test replenish function
cd ../replenish_promo_code_lambda
python test_local.py
```

## 📈 Monitoring

### CloudWatch Logs
```bash
# Monitor API calls
aws logs tail /aws/lambda/get-promo-code --follow

# Monitor replenish function
aws logs tail /aws/lambda/replenish-promo-code --follow
```

### S3 State Monitoring
```bash
# Check available codes
aws s3 cp s3://lambda-promo-sessions/promo-codes/available_codes.json -

# Check usage statistics
aws s3 cp s3://lambda-promo-sessions/promo-codes/used_codes_count.json -
```

## 🔄 Auto-Replenish System

The system automatically triggers replenishment when:
- Available codes drop below 3 for any amount
- Uses Playwright browser automation
- Scrapes BON admin panel for active codes
- Transfers codes to S3 for API consumption

### Replenish Flow
1. **Trigger**: API detects low code count
2. **Launch**: Browser automation starts
3. **Login**: Authenticates with BON admin
4. **Scrape**: Finds active promo codes
5. **Transfer**: Saves codes to S3
6. **Cleanup**: Updates counters and logs

## 🛡 Security

- **Environment variables** for sensitive data
- **IAM roles** with minimal permissions
- **VPC configuration** for network isolation
- **Encrypted S3 storage**
- **API rate limiting**

## 📝 Maintenance

### Regular Tasks
- Monitor CloudWatch logs for errors
- Check S3 storage usage
- Update BON credentials as needed
- Review API performance metrics

### Troubleshooting
- Check Lambda execution logs
- Verify S3 bucket permissions
- Test BON admin connectivity
- Validate API Gateway configuration

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📄 License

This project is proprietary software. All rights reserved.

## 📞 Support

For issues and questions:
- Check CloudWatch logs first
- Review API documentation
- Test with provided test scripts
- Contact development team

---

**Status**: ✅ Production Ready | **Last Updated**: July 2025
