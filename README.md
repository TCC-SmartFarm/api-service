# REST API (Go + Fiber)

### Descrição
Este microserviço atua como a interface de comunicação entre o **Front-end (React)** e a camada de persistência temporal (**InfluxDB Cloud**). Ele fornece endpoints seguros para a recuperação de séries temporais e estados atuais dos sensores.

### Bibliotecas
- **Fiber (gofiber/fiber)**: Framework web focado em performance extrema e baixo consumo de recursos.
- **InfluxDB Client Go**: Para execução de queries complexas e agregações de dados no tempo.

### Justificativa (ODS 9)
O uso de uma API desacoplada garante que o sistema de visualização possa evoluir independentemente da camada de sensores. A escolha do Go permite que a API responda com latência mínima, crucial para o monitoramento em tempo real de ativos agrícolas críticos.



### Endpoints Principais

1. **GET `/api/sensors/:userID/:days/:sensorID`**
   - **Fonte**: InfluxDB Cloud (:userID).
   - **Escopo**: Dados históricos do sensor(:sensorId) dos últimos dias (:days).
   - **Objetivo**: Análise de tendências e suporte à decisão de longo prazo.

2. **GET `/api/sensors/latest/:userID`**
   - **Fonte**: RabbitMQ (Fila `fila_{user_id}`).
   - **Escopo**: Últimas 10 mensagens trafegadas no barramento.
   - **Objetivo**: Visualização de status imediato e depuração de conectividade em campo.
