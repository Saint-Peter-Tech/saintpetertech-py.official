df_grupo <- rbind.data.frame(teste, teste2)

df_grupo$modulos_ativos <- 
  (df_grupo$bpm_status == "Ativo") +
  (df_grupo$pa_status == "Ativo") +
  (df_grupo$spo2_status == "Ativo") +
  (df_grupo$resp_status == "Ativo") +
  (df_grupo$temperatura_status == "Ativo") +
  (df_grupo$pic_status == "Ativo") +
  (df_grupo$pvc_status == "Ativo") +
  (df_grupo$ecg_status == "Ativo") +
  (df_grupo$etco2_status == "Ativo")


correlacao_cpu_mod <- cor(df_grupo$modulos_ativos,
                          df_grupo$cpu_percent)

modelo <- lm(cpu_percent ~ modulos_ativos,
             data = df_grupo)


plot(df_grupo$modulos_ativos, df_grupo$cpu_percent,
     pch=16,
     col="blue",
     xlab="Quantidade de módulos ativos",
     ylab="CPU %",
     main=paste("CPU x Módulos (correlação =", round(correlacao_cpu_mod,2), ")"))
abline(modelo, col="red", lwd=2)

#----------Módulos X RAM ---------------------------------------

correlacao_ram_mod <- cor(df_grupo$modulos_ativos, df_grupo$ram_percent)

modelo_ram <- lm(ram_percent ~ modulos_ativos, data = df_grupo)

plot(df_grupo$modulos_ativos, df_grupo$ram_percent,
     col="darkgreen",
     xlab="Quantidade de módulos ativos",
     ylab="RAM %",
     main=paste("RAM X Módulos (correlação =", round(correlacao_ram_mod, 2), ")"))
abline(modelo_ram, col="red", lwd=2)

