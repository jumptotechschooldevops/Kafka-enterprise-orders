FROM node:20 AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:latest
COPY nginx.conf.template /etc/nginx/nginx.conf.template
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
CMD ["/bin/sh", "-c", "envsubst '${BACKEND_URL}' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf && nginx -g 'daemon off;'"]

