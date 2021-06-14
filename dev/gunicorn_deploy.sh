gunicorn main:app -b 0.0.0.0:5000 \
  --log-level debug \
  --timeout 45 \
  --workers $(( 2*$(grep -c ^processor /proc/cpuinfo)+1 ))
