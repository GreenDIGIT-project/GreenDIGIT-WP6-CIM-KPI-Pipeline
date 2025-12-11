## 🌱🌍♻️ GreenDIGIT WP6.1 CIM & KPI Pipeline

### Overview

This is a configuration repository to spin-up the pipeline used in WP6 to ingest, process and publish metrics using CIM unified namespaces and integrated .

Related repositories:
- [GreenDIGIT-CIM](https://github.com/g-uva/GreenDIGIT-CIM)
- [GreenDIGIT-AuthServer](https://github.com/g-uva/GreenDIGIT-AuthServer)
- [GreenDIGIT-SQLAdapter](https://github.com/g-uva/GreenDIGIT-SQLAdapter)
- [GreenDIGIT-KPICalc](https://github.com/g-uva/GreenDIGIT-KPICalc)

*This work is funded from the European Union’s Horizon Europe research and innovation programme through the [GreenDIGIT project](https://greendigit-project.eu/), under the grant agreement No. [101131207](https://cordis.europa.eu/project/id/101131207)*.

<!-- ![GreenDIGIT Logo](auth_metrics_server/static/cropped-GD_logo.png)
![EU Logo](auth_metrics_server/static/EN-Funded-by-the-EU-POS-2.png) -->

<div style="display:flex;align-items:center;width:100%;">
  <img src="static/EN-Funded-by-the-EU-POS-2.png" alt="EU Logo" width="250px">
  <img src="static/cropped-GD_logo.png" alt="GreenDIGIT Logo" width="110px" style="margin-right:100px">
</div>

<!-- ## Usage
```bash
docker compose up -d --build
``` -->

## To install on-premises
1. Set `.env` file with values.
- [ ] Set values.
2. Install and set Nginx + HTTPS SSL certificate
- [ ] Reverse proxy settings for main port + SSL certificate steps.
3. Install Docker and `docker compose up -d --build`
4. Cronjob
5. MongoDB multi-cluster replica/failover
- [ ] Allow `27017` port for inbound/outbound traffic.
6. 

## Contact & Questions
**Contact:**  
For questions or to request access, please contact the GreenDIGIT UvA team:
- Gonçalo Ferreira: g.j.teixeiradepinhoferreira@uva.nl
- Adnan Tahir: a.tahir2@uva.nl
#
