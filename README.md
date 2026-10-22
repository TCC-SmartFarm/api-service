# REST API (Go + Fiber)

### Descrição
Este microserviço atua como a interface de comunicação entre o **Front-end (React)** e a camada de persistência temporal (**InfluxDB Cloud**). Ele fornece endpoints seguros para a recuperação de séries temporais e estados atuais dos sensores.

### Bibliotecas
- **Fiber (gofiber/fiber)**: Framework web focado em performance extrema e baixo consumo de recursos.
- **InfluxDB Client Go**: Para execução de queries complexas e agregações de dados no tempo.

### Justificativa (ODS 9)
O uso de uma API desacoplada garante que o sistema de visualização possa evoluir independentemente da camada de sensores. A escolha do Go permite que a API responda com latência mínima, crucial para o monitoramento em tempo real de ativos agrícolas críticos.



### Endpoints Principais

1. **GET `/api/sensors/influx/:userId/:days/:devEUI`**
   - **Fonte**: InfluxDB Cloud (:userId).
   - **Escopo**: Dados históricos do sensor(:devEUI) dos últimos dias (:days).
   - **Objetivo**: Análise de tendências e suporte à decisão de longo prazo.

2. **GET `/api/sensors/latest/:userId/:devEUI`**
   - **Fonte**: REDIS cache (`userId:%s:devEUI:%s:history`).
   - **Escopo**: Últimas 20 mensagens trafegadas no cache.
   - **Objetivo**: Visualização de status imediato e depuração de conectividade em campo.

3. **GET `/api/sensors/all/:userId`**
   - **Fonte**: REDIS cache (`userId:%s:devEUI:*:history`).
   - **Escopo**: Última mensagen de cada um dos sensores do usuario trafegadas no cache.
   - **Objetivo**: Visualização de status imediato e depuração de conectividade em campo.

4. **GET `/api/v1/irrigation?userId=:userId&devEUI=:devEUI&timezone=America%2FSao_Paulo`**
   - **Fonte**: Microsserviço de recomendação de irrigação (`Irrigacao-MSS`), que integra dados da API de sensores, localização dos dispositivos no Supabase e previsão meteorológica do Open-Meteo.
   - **Escopo**: Recupera as leituras recentes e válidas do sensor, apresenta o histórico disponível, estima a tendência da umidade do solo para as próximas 24 horas e consulta a previsão meteorológica para seis dias.
   - **Objetivo**: Apoiar a tomada de decisão sobre a irrigação, combinando a umidade atual do solo, sua tendência estimada e a previsão de precipitação para indicar se a irrigação deve ser evitada, avaliada manualmente ou recomendada para um dia mais adequado.

   - **Parâmetros**:
     - `userId` (obrigatório): identificador do usuário associado ao dispositivo.
     - `devEUI` (obrigatório): identificador único do sensor consultado.
     - `timezone` (opcional): fuso horário utilizado no processamento das datas. O padrão é `America/Sao_Paulo`.

   - **Observação**: O endpoint retorna uma recomendação estruturada em JSON e não aciona diretamente os equipamentos de irrigação. A decisão é baseada em regras configuráveis, estimativas estatísticas e dados meteorológicos, podendo exigir validação manual quando houver incerteza ou indícios de anomalia nas leituras.