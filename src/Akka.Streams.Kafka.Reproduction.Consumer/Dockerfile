FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app

ENV KAFKA_HOST "kafka"

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build

ENV KAFKA_HOST "kafka"

WORKDIR /src
COPY ["src/Akka.Streams.Kafka.Reproduction.Consumer/Akka.Streams.Kafka.Reproduction.Consumer.csproj", "Akka.Streams.Kafka.Reproduction.Consumer/"]
RUN dotnet restore "src/Akka.Streams.Kafka.Reproduction.Consumer/Akka.Streams.Kafka.Reproduction.Consumer.csproj"
COPY . .
WORKDIR "/src/Akka.Streams.Kafka.Reproduction.Consumer"
RUN dotnet build "Akka.Streams.Kafka.Reproduction.Consumer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Akka.Streams.Kafka.Reproduction.Consumer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Akka.Streams.Kafka.Reproduction.Consumer.dll"]
