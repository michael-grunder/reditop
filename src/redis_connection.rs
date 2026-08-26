use redis::{AsyncConnectionConfig, Client, ErrorKind};

use crate::model::{RuntimeSettings, Target, TargetProtocol};

/// Open a Redis connection and authenticate it explicitly when credentials are
/// present. Explicit AUTH lets us retry the pre-ACL password-only form when an
/// older Redis server rejects `AUTH default <password>`.
pub async fn connect(
    target: &Target,
    settings: &RuntimeSettings,
) -> redis::RedisResult<redis::aio::MultiplexedConnection> {
    let client = Client::open(connection_url(target))?;
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(Some(settings.connect_timeout))
        .set_response_timeout(Some(settings.command_timeout));
    let mut connection = client
        .get_multiplexed_async_connection_with_config(&config)
        .await?;

    authenticate(&mut connection, target).await?;
    Ok(connection)
}

async fn authenticate(
    connection: &mut impl redis::aio::ConnectionLike,
    target: &Target,
) -> redis::RedisResult<()> {
    let Some(password) = target.password.as_deref() else {
        return Ok(());
    };
    let username = target.username.as_deref().unwrap_or("default");

    let acl_result = redis::cmd("AUTH")
        .arg(username)
        .arg(password)
        .query_async::<String>(connection)
        .await;

    match acl_result {
        Ok(_) => Ok(()),
        Err(error) if username == "default" && should_try_legacy_auth(&error) => redis::cmd("AUTH")
            .arg(password)
            .query_async::<String>(connection)
            .await
            .map(|_| ()),
        Err(error) => Err(error),
    }
}

fn should_try_legacy_auth(error: &redis::RedisError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    matches!(
        error.kind(),
        ErrorKind::Server(redis::ServerErrorKind::ResponseError) | ErrorKind::Extension
    ) && (message.contains("wrong number of arguments") || message.contains("syntax error"))
}

fn connection_url(target: &Target) -> String {
    match target.protocol {
        TargetProtocol::Tcp => format!("redis://{}/", target.addr),
        TargetProtocol::Unix => format!("redis+unix://{}", target.addr),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use redis::{Cmd, ErrorKind, Pipeline, RedisFuture, Value};

    use super::{authenticate, connection_url, should_try_legacy_auth};
    use crate::model::{Target, TargetProtocol};

    fn target(protocol: TargetProtocol, addr: &str) -> Target {
        Target {
            alias: None,
            addr: addr.to_string(),
            protocol,
            username: Some("default".to_string()),
            password: Some("secret:/?".to_string()),
            tags: Vec::new(),
            process_id: None,
        }
    }

    struct MockConnection {
        commands: Vec<Vec<u8>>,
        responses: VecDeque<redis::RedisResult<Value>>,
    }

    impl redis::aio::ConnectionLike for MockConnection {
        fn req_packed_command<'a>(&'a mut self, command: &'a Cmd) -> RedisFuture<'a, Value> {
            self.commands.push(command.get_packed_command());
            let response = self.responses.pop_front().expect("mock response");
            Box::pin(async move { response })
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            _pipeline: &'a Pipeline,
            _offset: usize,
            _count: usize,
        ) -> RedisFuture<'a, Vec<Value>> {
            Box::pin(async { panic!("authentication does not use pipelines") })
        }

        fn get_db(&self) -> i64 {
            0
        }
    }

    #[test]
    fn connection_url_never_contains_credentials() {
        assert_eq!(
            connection_url(&target(TargetProtocol::Tcp, "127.0.0.1:6380")),
            "redis://127.0.0.1:6380/"
        );
        assert_eq!(
            connection_url(&target(TargetProtocol::Unix, "/tmp/redis.sock")),
            "redis+unix:///tmp/redis.sock"
        );
    }

    #[test]
    fn legacy_fallback_is_limited_to_unsupported_acl_syntax() {
        let old_redis = redis::RedisError::from((
            ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "ERR wrong number of arguments for 'auth' command",
        ));
        let wrong_password = redis::RedisError::from((
            ErrorKind::AuthenticationFailed,
            "WRONGPASS invalid username-password pair",
        ));

        assert!(should_try_legacy_auth(&old_redis));
        assert!(!should_try_legacy_auth(&wrong_password));
    }

    #[tokio::test]
    async fn default_user_retries_with_password_only_for_old_redis() {
        let old_redis = redis::RedisError::from((
            ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "ERR wrong number of arguments for 'auth' command",
        ));
        let mut connection = MockConnection {
            commands: Vec::new(),
            responses: VecDeque::from([Err(old_redis), Ok(Value::Okay)]),
        };
        let target = target(TargetProtocol::Tcp, "127.0.0.1:6380");

        authenticate(&mut connection, &target)
            .await
            .expect("legacy AUTH should succeed");

        assert_eq!(connection.commands.len(), 2);
        assert_eq!(
            connection.commands[0],
            b"*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$9\r\nsecret:/?\r\n"
        );
        assert_eq!(
            connection.commands[1],
            b"*2\r\n$4\r\nAUTH\r\n$9\r\nsecret:/?\r\n"
        );
    }
}
