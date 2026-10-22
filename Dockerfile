# Shared multi-stage Dockerfile for every .NET service.
# Usage: docker build --build-arg PROJECT=Orders.Api .
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /repo
COPY Directory.Build.props Directory.Packages.props ./
COPY src/ src/
RUN dotnet publish "src/${PROJECT}/${PROJECT}.csproj" -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
ARG PROJECT
ENV APP_DLL=${PROJECT}.dll \
    ASPNETCORE_URLS=http://+:8080
WORKDIR /app
COPY --from=build /app .
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=20 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080' || exit 1
ENTRYPOINT ["sh", "-c", "exec dotnet \"$APP_DLL\""]
