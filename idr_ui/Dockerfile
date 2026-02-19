# Stage 1: Build
FROM node:20.18.1-alpine AS builder

WORKDIR /app

# Install dependencies reproducibly from lockfile
COPY package*.json ./
RUN npm ci

ARG VITE_AUTH_AUTHORITY
ARG VITE_AUTH_CLIENT_ID
ARG VITE_API_BASE_URL
ARG VITE_ALLOW_INSECURE_DEV_AUTH
ENV VITE_AUTH_AUTHORITY=$VITE_AUTH_AUTHORITY
ENV VITE_AUTH_CLIENT_ID=$VITE_AUTH_CLIENT_ID
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL
ENV VITE_ALLOW_INSECURE_DEV_AUTH=$VITE_ALLOW_INSECURE_DEV_AUTH

# Copy source and build
COPY . .
RUN npm run build

# Stage 2: Serve
FROM nginx:1.27.3-alpine

# Remove default nginx static assets
RUN rm -rf /usr/share/nginx/html/*

# Copy static assets from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy custom nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
