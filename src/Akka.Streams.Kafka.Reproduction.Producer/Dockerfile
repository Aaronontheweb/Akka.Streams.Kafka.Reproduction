FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app

ENV KAFKA_HOST "kafka"
ENV KAFKA_PORT "29092"
ENV KAFKA_SASL_USERNAME ""
ENV KAFKA_SASL_PASSWORD ""

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["src/Akka.Streams.Kafka.Reproduction.Producer/Akka.Streams.Kafka.Reproduction.Producer.csproj", "Akka.Streams.Kafka.Reproduction.Producer/"]
RUN dotnet restore "src/Akka.Streams.Kafka.Reproduction.Producer/Akka.Streams.Kafka.Reproduction.Producer.csproj"
COPY . .
WORKDIR "/src/Akka.Streams.Kafka.Reproduction.Producer"
RUN dotnet build "Akka.Streams.Kafka.Reproduction.Producer.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Akka.Streams.Kafka.Reproduction.Producer.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Akka.Streams.Kafka.Reproduction.Producer.dll"]
