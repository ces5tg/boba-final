FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build-env
WORKDIR /src
COPY TodoApi.csproj .
RUN dotnet restore
COPY . .
RUN dotnet publish -c Release -o /publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
#loq eu hace esta linea es publicar applicaion y crear un archivo ejecutable 
WORKDIR /publish
COPY --from=build-env /publish .
EXPOSE 8080
ENTRYPOINT ["dotnet", "TodoApi.dll"]
    