#!/usr/bin/bash

# Create RSA keys
cd dev_utils || exit 1
mkdir -p keys/pub || exit 1
cd keys || exit 1

ssh-keygen -t rsa -b 4096 -m PEM -f example.demo.pem -q -N ""
openssl rsa -in example.demo.pem -pubout -outform PEM -out pub/example.demo.pub

# Shared content to use as template
header_template='{
    "typ": "JWT",
    "kid": "0001"
}'

build_header() {
        jq -c \
                --arg iat_str "$(date +%s)" \
                --arg alg "${1}" \
                '
        ($iat_str | tonumber) as $iat
        | .alg = $alg
        | .iat = $iat
        | .exp = ($iat + 86400)
        ' <<<"$header_template" | tr -d '\n'
}

b64enc() { openssl enc -base64 -A | tr '+/' '-_' | tr -d '='; }
json() { jq -c . | LC_CTYPE=C tr -d '\n'; }
rs_sign() { openssl dgst -binary -sha"${1}" -sign <(printf '%s\n' "$2"); }
es_sign() { openssl dgst -binary -sha"${1}" -sign <(printf '%s\n' "$2") | openssl asn1parse -inform DER | grep INTEGER | cut -d ':' -f 4 | xxd -p -r; }

sign() {
        if [ -n "$2" ]; then
                rsa_secret=$(<"$2")
        else
                echo "no signing key supplied"
                exit 1
        fi
        local algo payload header sig secret=$rsa_secret
        algo=${1:-RS256}
        algo=${algo^^}
        header=$(build_header "$algo") || return
        payload=${4:-$test_payload}
        signed_content="$(json <<<"$header" | b64enc).$(json <<<"$payload" | b64enc)"
        case $algo in
        RS*) sig=$(printf %s "$signed_content" | rs_sign "${algo#RS}" "$secret" | b64enc) ;;
        ES*) sig=$(printf %s "$signed_content" | es_sign "${algo#ES}" "$secret" | b64enc) ;;
        *)
                echo "Unknown algorithm" >&2
                return 1
                ;;
        esac
        printf '%s.%s\n' "${signed_content}" "${sig}"
}

iat=$(date +%s)
exp=$(date --date="${3:-tomorrow}" +%s)

test_payload='{
  "sub": "test",
  "azp": "azp",
  "scope": "openid ga4gh_passport_v1",
  "iss": "http://example.demo",
  "exp": '"$exp"',
  "iat": '"$iat"',
  "jti": "6ad7aa42-3e9c-4833-bd16-765cb80c2102"
}'

sign RS256 example.demo.pem > token.jwt
