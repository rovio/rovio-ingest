#
# Copyright 2026 Rovio Entertainment Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import base64

import pytest
from authlib.jose import JsonWebSignature
from authlib.jose.errors import BadSignatureError


@pytest.mark.parametrize('key', [None, b'regression-test-key'])
def test_empty_jws_signatures_are_rejected(key):
    payload = base64.urlsafe_b64encode(b'{"sub":"attacker"}').rstrip(b'=').decode()
    forged = {'payload': payload, 'signatures': []}

    with pytest.raises(BadSignatureError):
        JsonWebSignature().deserialize_json(forged, key=key)


def test_signed_jws_json_is_verified():
    jws = JsonWebSignature()
    key = b'regression-test-key'
    payload = b'{"sub":"user"}'
    headers = [{'protected': {'alg': 'HS256'}}] * 2
    signed = jws.serialize_json(headers, payload, key)

    assert jws.deserialize_json(signed, key)['payload'] == payload

    signature = signed['signatures'][0]['signature']
    signed['signatures'][0]['signature'] = ('A' if signature[0] != 'A' else 'B') + signature[1:]
    with pytest.raises(BadSignatureError):
        jws.deserialize_json(signed, key)
