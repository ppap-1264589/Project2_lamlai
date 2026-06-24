#!/bin/bash
set -e

# Chỉ init lần đầu tiên tạo container
if [ ! -f /app/superset_home/.initialized ]; then
  echo "⏳ Lần đầu khởi động — đang khởi tạo Superset..."

  superset db upgrade

  superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname Admin \
    --email admin@example.com \
    --password admin

  superset init

  # Đánh dấu đã init xong
  touch /app/superset_home/.initialized
  echo "✅ Khởi tạo xong!"

else
  echo "✅ Superset đã được khởi tạo trước đó — bỏ qua"
fi

exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 2 \
  --timeout 120 \
  "superset.app:create_app()"