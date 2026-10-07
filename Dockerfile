# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
# LatticeDB v0.10.0 is pure Go; no CGO or native runtime libraries are needed.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -trimpath -o /conductor ./cmd/conductor

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
COPY --from=build /conductor /usr/local/bin/conductor
COPY LICENSE /usr/share/licenses/conductor/LICENSE
ENV CONDUCTOR_DB=/data/conductor.db
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["conductor"]
