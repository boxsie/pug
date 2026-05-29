# Build the PugPong matchmaker. Context for this build is the PUG repo root
# (set in docker-compose.yml's `context: ../..`) so the ProjectReferences into
# src/PUG.Core and src/PUG.Ensemble resolve cleanly. Ensemble.Client is pulled
# from NuGet during restore, so this builds self-contained inside the container.

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish samples/PugPong/Matchmaker/PugPong.Matchmaker.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
ENV ENSEMBLE_GRPC_ADDR=http://ensembled:9090
ENTRYPOINT ["dotnet", "PugPong.Matchmaker.dll"]
