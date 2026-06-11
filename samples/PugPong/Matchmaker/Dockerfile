# Build the PugPong matchmaker. Context for this build is the PUG repo root
# (set in docker-compose.yml's `context: ../..`) so the ProjectReferences into
# src/PUG.Core and src/PUG.Ensemble resolve cleanly. Ensemble.Client is pulled
# from NuGet during restore, so this builds self-contained inside the container.
#
# SDK 10 (not 8) because the repo-root global.json pins sdk 10.0.107
# (rollForward: latestFeature) — an 8.0 SDK image fails resolution before it
# can publish. The matchmaker still targets net8.0, so the runtime stage below
# stays on the .NET 8 ASP.NET runtime.

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish samples/PugPong/Matchmaker/PugPong.Matchmaker.csproj \
    -c Release -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .
ENV ENSEMBLE_GRPC_ADDR=http://ensembled:9090
ENTRYPOINT ["dotnet", "PugPong.Matchmaker.dll"]
