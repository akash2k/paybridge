FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/PayBridge.Shared/PayBridge.Shared.csproj src/PayBridge.Shared/
COPY src/PayBridge.PaymentApi/PayBridge.PaymentApi.csproj src/PayBridge.PaymentApi/
RUN dotnet restore src/PayBridge.PaymentApi/PayBridge.PaymentApi.csproj
COPY src/ src/
RUN dotnet publish src/PayBridge.PaymentApi/PayBridge.PaymentApi.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "PayBridge.PaymentApi.dll"]
