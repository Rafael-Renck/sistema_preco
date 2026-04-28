def test_login_flow_redirects_to_password_change_when_reset_is_required(app_ctx):
    application = app_ctx.app
    client = application.test_client()
    Usuario = app_ctx.Usuario
    db = app_ctx.db
    app_ctx._register_audit = lambda *args, **kwargs: None

    with application.app_context():
        usuario = Usuario.query.filter_by(email='admin@local').first()
        if usuario is None:
            usuario = Usuario(
                nome='Administrador',
                email='admin@local',
                senha=app_ctx._hash_password('Admin@123'),
                perfil='adm',
            )
            db.session.add(usuario)
        else:
            usuario.nome = 'Administrador'
            usuario.senha = app_ctx._hash_password('Admin@123')
            usuario.perfil = 'adm'
        usuario.must_reset_senha = True
        usuario.acesso_insumos = True
        usuario.acesso_consulta = True
        usuario.acesso_contratos = True
        usuario.acesso_tuss_rol = True
        db.session.commit()

    response_get = client.get('/login')
    assert response_get.status_code == 200
    assert b'name="email"' in response_get.data

    response_post = client.post(
        '/login',
        data={'email': 'admin@local', 'senha': 'Admin@123'},
        follow_redirects=False,
    )

    assert response_post.status_code == 302
    assert response_post.headers['Location'].endswith('/minha-senha')

    with client.session_transaction() as sess:
        assert sess['user_id'] > 0
        assert sess['nome'] == 'Administrador'
        assert sess['perfil'] == 'adm'
        assert sess['must_change_senha'] is True


def test_anonymous_dashboard_redirects_to_login(app_ctx):
    application = app_ctx.app
    client = application.test_client()

    response = client.get('/', follow_redirects=False)

    assert response.status_code == 302
    assert response.headers['Location'].endswith('/login')
