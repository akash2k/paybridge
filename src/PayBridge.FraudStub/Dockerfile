FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY src/PayBridge.Shared/PayBridge.Shared.csproj src/PayBridge.Shared/
COPY src/PayBridge.FraudStub/PayBridge.FraudStub.csproj src/PayBridge.FraudStub/
RUN dotnet restore src/PayBridge.FraudStub/PayBridge.FraudStub.csproj
COPY src/ src/
RUN dotnet publish src/PayBridge.FraudStub/PayBridge.FraudStub.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/publish .
EXPOSE 8081
ENV ASPNETCORE_URLS=http://+:8081
ENTRYPOINT ["dotnet", "PayBridge.FraudStub.dll"]
