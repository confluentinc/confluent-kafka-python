# Copyright 2026 Confluent Inc.
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

"""
Shared base for the confluent-kafka-python verifiable clients used by
Apache Kafka's Ducktape system tests.

Each verifiable client emits newline-delimited JSON events on stdout and
accepts a standardized CLI so the Python harness in
``apache/kafka/tests/kafkatest/services/verifiable_*.py`` can parse the
event stream and verify client behavior.

The base provides:

  * :meth:`emit_event` / :meth:`send` — thread-safe newline-delimited JSON
    emission with an epoch-millisecond ``timestamp`` (matching Java's
    ``System.currentTimeMillis``). The client invokes our delivery /
    acknowledgement callbacks from within ``poll`` / ``commit_*`` on the
    application thread, but a lock still guards stdout so nothing interleaves
    a JSON line.
  * SIGTERM/SIGINT handling that clears :attr:`run`; main loops poll it.
  * Java-properties parsing (``--command-config``) and ``-X key=value``
    overrides, with Java -> librdkafka config-key translation and
    JAAS/``KAFKA_OPTS`` credential extraction.
"""

import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time


class VerifiableClient(object):
    """Generic base for a kafkatest verifiable client: event emission,
    signal handling, and Java -> librdkafka config translation."""

    def __init__(self, conf):
        super(VerifiableClient, self).__init__()
        self.conf = conf
        self.conf.setdefault('client.id', 'python@' + socket.gethostname())
        self.run = True
        self._stdout_lock = threading.Lock()
        signal.signal(signal.SIGTERM, self._sig_handler)
        signal.signal(signal.SIGINT, self._sig_handler)
        self.dbg('Pid is %d' % os.getpid())

    def _sig_handler(self, sig, frame):
        self.dbg('Signal %d received, terminating' % sig)
        self.run = False

    @staticmethod
    def _now_ms():
        """Wallclock milliseconds since the Unix epoch (matches Java
        System.currentTimeMillis and librdkafka's now_ms)."""
        return int(time.time() * 1000)

    @staticmethod
    def _logtime():
        return time.strftime('%H:%M:%S', time.localtime())

    def dbg(self, s):
        """Debugging printout to stderr."""
        sys.stderr.write('%% %s DEBUG: %s\n' % (self._logtime(), s))

    def err(self, s, term=False):
        """Error printout to stderr; if term=True the process exits."""
        sys.stderr.write('%% %s ERROR: %s\n' % (self._logtime(), s))
        if term:
            sys.stderr.write('%% FATAL ERROR ^\n')
            sys.exit(1)

    def send(self, d):
        """Emit a dict as one newline-delimited JSON event on stdout for the
        kafkatest harness. Adds the ``timestamp`` field (epoch ms) that every
        event in the contract carries. Serialized across threads so
        callback-emitted events never interleave with the main loop's."""
        d['timestamp'] = self._now_ms()
        line = json.dumps(d)
        with self._stdout_lock:
            sys.stdout.write('%s\n' % line)
            sys.stdout.flush()

    def emit_event(self, name):
        """Emit a name-only event: {"name": <name>, "timestamp": <ms>}."""
        self.send({'name': name})

    # ------------------------------------------------------------------ #
    # Configuration handling                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def set_config(conf, args):
        """Set client config properties from the parsed ``args`` dict.

        Keys prefixed ``conf_`` become librdkafka config keys (after Java ->
        librdkafka translation); ``topicconf_`` keys are passed through as-is.
        Everything else is application config and ignored here.
        """
        for n, v in args.items():
            if v is None:
                continue

            if n.startswith('topicconf_'):
                conf[n[len('topicconf_'):]] = v
                continue

            if not n.startswith('conf_'):
                continue  # Application config; not a client property.

            VerifiableClient._apply_translated(conf, n[len('conf_'):], v)

    @staticmethod
    def _apply_translated(conf, key, value):
        """Apply one key=value to ``conf``, translating Java-style keys to
        their librdkafka equivalents."""

        # Java-only keys that librdkafka neither needs nor understands.
        if key in ('ssl.truststore.type',
                   'ssl.keystore.type',
                   'sasl.mechanism.inter.broker.protocol',
                   'sasl.kerberos.service.name'):
            return

        if key == 'partition.assignment.strategy':
            # "org.apache.kafka.clients.consumer.RangeAssignor" -> "range"
            value = re.sub(
                r'org\.apache\.kafka\.clients\.consumer\.(\w+)Assignor',
                lambda m: m.group(1).lower(), value)
            if value == 'sticky':
                value = 'cooperative-sticky'

        # Java emits HTTPS / "" ; librdkafka's enum is lowercase https/none.
        if key == 'ssl.endpoint.identification.algorithm':
            value = value.lower() if value else 'none'

        # JAAS config: pull username/password out for PLAIN/SCRAM.
        if key == 'sasl.jaas.config':
            creds = VerifiableClient._extract_jaas_creds(value)
            if creds is not None:
                conf['sasl.username'], conf['sasl.password'] = creds
                return
            # else fall through and set verbatim.

        conf[key] = value

    @staticmethod
    def _extract_jaas_creds(jaas):
        """Return (username, password) parsed from a JAAS config string, or
        None if either field is absent."""
        user = re.search(r'username="([^"]*)"', jaas)
        pwd = re.search(r'password="([^"]*)"', jaas)
        if user and pwd:
            return user.group(1), pwd.group(1)
        return None

    @staticmethod
    def read_config_file(path):
        """Read a Java properties file and return a dict of properties.

        Lines starting with '#' or '!' are comments; blank lines are skipped;
        the key/value separator is the first '=' or ':'. Lenient: malformed
        lines without a separator are skipped (as Java Properties does).
        """
        conf = {}
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line[0] in ('#', '!'):
                    continue
                seps = [line.find(c) for c in '=:' if c in line]
                sep = min(seps) if seps else -1
                if sep < 1:
                    # Malformed line; skip silently (Java Properties does too).
                    continue
                conf[line[:sep].strip()] = line[sep + 1:].strip()
        return conf

    @classmethod
    def build_conf(cls, args, config_key):
        """Build a librdkafka config dict from parsed ``args``.

        Resolution order (later wins): ``conf_``/``topicconf_`` flags, then a
        ``--command-config`` / ``--producer.config`` properties file (under
        ``args[config_key]``), then ``-X key=value`` overrides
        (``args['extra_conf']``), then JAAS credentials from ``KAFKA_OPTS``.
        Returns the assembled dict; the caller constructs the client with it.
        """
        conf = {}
        cls.set_config(conf, args)

        path = args.get(config_key)
        if path:
            file_args = {'conf_' + k: v
                         for k, v in cls.read_config_file(path).items()}
            cls.set_config(conf, file_args)

        for entry in args.get('extra_conf', []):
            # argparse stores each -X as a one-element list.
            kv = entry[0] if isinstance(entry, (list, tuple)) else entry
            if '=' not in kv:
                raise SystemExit(
                    'Malformed -X property: %s (expected key=value)' % kv)
            k, v = kv.split('=', 1)
            cls._apply_translated(conf, k.strip(), v.strip())

        cls._apply_jaas_from_kafka_opts_into(conf)
        cls._convert_java_keystores(conf)
        return conf

    # ------------------------------------------------------------------ #
    # Java keystore/truststore -> PEM conversion                         #
    #                                                                    #
    # librdkafka cannot read Java JKS (or PKCS12) keystores/truststores. #
    # The harness's security_config hands us Java stores via             #
    # ssl.{truststore,keystore}.location; convert them to the PEM files  #
    # librdkafka expects and rewrite the conf keys in place.             #
    # ------------------------------------------------------------------ #

    @classmethod
    def _convert_java_keystores(cls, conf):
        """Rewrite Java truststore/keystore config into librdkafka PEM config.

        Truststore -> ssl.ca.location (CA cert bundle).
        Keystore   -> ssl.certificate.location + ssl.key.location (client
                      cert + unencrypted private key).

        Java-only keys are dropped. A no-op when no Java stores are present
        (e.g. PLAINTEXT/SASL_PLAINTEXT). Raises SystemExit on conversion
        failure so the client fails loudly rather than starting misconfigured.
        """
        truststore = conf.pop('ssl.truststore.location', None)
        truststore_pwd = conf.pop('ssl.truststore.password', None)
        keystore = conf.pop('ssl.keystore.location', None)
        keystore_pwd = conf.pop('ssl.keystore.password', None)
        key_pwd = conf.get('ssl.key.password')
        # Drop the Java-only store-type hints; librdkafka infers from content.
        conf.pop('ssl.truststore.type', None)
        conf.pop('ssl.keystore.type', None)

        if not truststore and not keystore:
            return

        outdir = tempfile.mkdtemp(prefix='cfk-ssl-')

        if truststore:
            ca_pem = os.path.join(outdir, 'ca.pem')
            cls._extract_ca_pem(truststore, truststore_pwd, ca_pem)
            conf['ssl.ca.location'] = ca_pem

        if keystore:
            cert_pem = os.path.join(outdir, 'cert.pem')
            key_pem = os.path.join(outdir, 'key.pem')
            cls._extract_keystore_pem(keystore, keystore_pwd, key_pwd,
                                      cert_pem, key_pem)
            conf['ssl.certificate.location'] = cert_pem
            conf['ssl.key.location'] = key_pem
            # The extracted key is unencrypted, so clear any key password.
            conf.pop('ssl.key.password', None)

    @staticmethod
    def _to_pkcs12(src, src_pwd, dst, dst_pwd):
        """Convert a JKS (or PKCS12) store to PKCS12 via keytool. Idempotent
        for an already-PKCS12 source. Raises SystemExit on failure."""
        cmd = [
            'keytool', '-importkeystore', '-noprompt',
            '-srckeystore', src, '-srcstorepass', src_pwd or '',
            '-destkeystore', dst, '-deststoretype', 'PKCS12',
            '-deststorepass', dst_pwd, '-srcstoretype', 'JKS',
        ]
        proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            # Source may already be PKCS12; retry without forcing srcstoretype.
            cmd_p12 = [
                'keytool', '-importkeystore', '-noprompt',
                '-srckeystore', src, '-srcstorepass', src_pwd or '',
                '-srcstoretype', 'PKCS12',
                '-destkeystore', dst, '-deststoretype', 'PKCS12',
                '-deststorepass', dst_pwd,
            ]
            proc = subprocess.run(cmd_p12, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT)
            if proc.returncode != 0:
                raise SystemExit(
                    'keytool failed converting %s to PKCS12:\n%s'
                    % (src, proc.stdout.decode('utf-8', 'replace')))

    @classmethod
    def _extract_ca_pem(cls, truststore, truststore_pwd, ca_pem_out):
        """Extract CA certificate(s) from a Java truststore into a PEM bundle.

        keytool JKS/PKCS12 -> PKCS12 -> `openssl pkcs12 -nokeys` (CA certs).
        """
        p12 = ca_pem_out + '.p12'
        cls._to_pkcs12(truststore, truststore_pwd, p12, 'changeit')
        cls._run_openssl([
            'openssl', 'pkcs12', '-in', p12, '-out', ca_pem_out,
            '-nokeys', '-passin', 'pass:changeit',
        ], 'truststore', truststore)

    @classmethod
    def _extract_keystore_pem(cls, keystore, keystore_pwd, key_pwd,
                              cert_pem_out, key_pem_out):
        """Extract the client cert + private key from a Java keystore into
        separate PEM files (key written unencrypted)."""
        p12 = cert_pem_out + '.p12'
        cls._to_pkcs12(keystore, keystore_pwd, p12, 'changeit')
        # Client certificate chain (no private key, no CA).
        cls._run_openssl([
            'openssl', 'pkcs12', '-in', p12, '-out', cert_pem_out,
            '-clcerts', '-nokeys', '-passin', 'pass:changeit',
        ], 'keystore cert', keystore)
        # Private key, decrypted (-nodes), so no ssl.key.password is needed.
        cls._run_openssl([
            'openssl', 'pkcs12', '-in', p12, '-out', key_pem_out,
            '-nocerts', '-nodes', '-passin', 'pass:changeit',
        ], 'keystore key', keystore)

    @staticmethod
    def _run_openssl(cmd, what, src):
        proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            raise SystemExit(
                'openssl failed extracting %s from %s:\n%s'
                % (what, src, proc.stdout.decode('utf-8', 'replace')))

    @classmethod
    def _apply_jaas_from_kafka_opts_into(cls, conf):
        """Static variant of :meth:`apply_jaas_from_kafka_opts` that writes
        into a plain conf dict (used before the client is constructed)."""
        opts = os.environ.get('KAFKA_OPTS')
        if not opts:
            return
        m = re.search(r'-Djava\.security\.auth\.login\.config=(\S+)', opts)
        if not m:
            return
        try:
            with open(m.group(1), 'r') as f:
                content = f.read()
        except OSError as e:
            sys.stderr.write('%% Failed to read JAAS file %s: %s\n'
                             % (m.group(1), e))
            return
        idx = content.find('KafkaClient')
        if idx < 0:
            return
        creds = cls._extract_jaas_creds(content[idx:])
        if creds is not None:
            conf['sasl.username'], conf['sasl.password'] = creds
