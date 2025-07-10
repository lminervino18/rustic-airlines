certs:
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -keyout cert.key -out cert.crt -nodes -days 36500 -subj "/CN=databaseserver" -addext "subjectAltName=DNS:databaseserver" -addext "basicConstraints=critical,CA:FALSE"

pk:
    openssl genpkey -algorithm RSA -out server.key -pkeyopt rsa_keygen_bits:2048

gen_csr:
    openssl req -new -key server.key -out server.csr

make_cert_for_seed:
    openssl req -x509 -nodes -newkey rsa:2048 -keyout private.key -out certificate.crt -days 365 -subj '/CN=127.0.0.1'
