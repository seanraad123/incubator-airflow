# Copyright 2016 Ananya Mishra (am747@cornell.edu)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log
log.critical('after login')
import flask_login

# Need to expose these downstream
# pylint: disable=unused-import
from flask_login import (current_user,
                         logout_user,
                         login_required,
                         login_user)
# pylint: enable=unused-import
log.critical('after flask_login')

from flask import url_for, redirect, request
log.critical('after flask others')

from flask_oauthlib.client import OAuth
log.critical('after flask oauth')

from airflow import models, configuration, settings
log.critical('after airflow imports')

from airflow.utils.db import provide_session

log.critical('after db imports')


def get_config_param(param):
    return str(configuration.get('google', param))


class GoogleUser(models.User):

    def __init__(self, user):
        self.user = user

    def is_active(self):
        '''Required by flask_login'''
        return True

    def is_authenticated(self):
        '''Required by flask_login'''
        return True

    def is_anonymous(self):
        '''Required by flask_login'''
        return False

    def get_id(self):
        '''Returns the current user id as required by flask_login'''
        return self.user.get_id()

    def data_profiling(self):
        '''Provides access to data profiling tools'''
        return True

    def is_superuser(self):
        '''Access all the things'''
        return True


class AuthenticationError(Exception):
    pass


class GoogleAuthBackend(object):

    def __init__(self):
        log.critical('GAB init')
        # self.google_host = get_config_param('host')
        self.login_manager = flask_login.LoginManager()
        self.login_manager.login_view = 'airflow.login'
        self.flask_app = None
        self.google_oauth = None
        self.api_rev = None
        log.critical('GAB done initing')

    def init_app(self, flask_app):
        log.critical('GAB init app call')
        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.google_oauth = OAuth(self.flask_app).remote_app(
            'google',
            consumer_key=get_config_param('client_id'),
            consumer_secret=get_config_param('client_secret'),
            request_token_params={'scope': [
                'https://www.googleapis.com/auth/userinfo.profile',
                'https://www.googleapis.com/auth/userinfo.email']},
            base_url='https://www.google.com/accounts/',
            request_token_url=None,
            access_token_method='POST',
            access_token_url='https://accounts.google.com/o/oauth2/token',
            authorize_url='https://accounts.google.com/o/oauth2/auth')

        self.login_manager.user_loader(self.load_user)

        log.critical('GAB init app near done')
        self.flask_app.add_url_rule(get_config_param('oauth_callback_route'),
                                    'google_oauth_callback',
                                    self.oauth_callback)

    def login(self, request):
        log.critical('GAB login')
        log.debug('Redirecting user to Google login')
        return self.google_oauth.authorize(callback=url_for(
            'google_oauth_callback',
            _external=True,
            next=request.args.get('next') or request.referrer or None))

    def get_google_user_profile_info(self, google_token):
        log.critical('get google call')
        resp = self.google_oauth.get('https://www.googleapis.com/oauth2/v1/userinfo',
                                    token=(google_token, ''))

        if not resp or resp.status != 200:
            raise AuthenticationError(
                'Failed to fetch user profile, status ({0})'.format(
                    resp.status if resp else 'None'))

        log.critical('get google out')
        return resp.data['name'], resp.data['email']

    def domain_check(self, email):
        log.critical('domain check')
        domain = email.split('@')[1]
        if domain == get_config_param('domain'):
            return True
        return False

    @provide_session
    def load_user(self, userid, session=None):
        log.critical('user load call')
        if not userid or userid == 'None':
            return None

        user = session.query(models.User).filter(
            models.User.id == int(userid)).first()
        log.critical('user load call done')
        return GoogleUser(user)

    @provide_session
    def oauth_callback(self, session=None):
        log.critical('callback call')
        log.debug('Google OAuth callback called')

        next_url = request.args.get('next') or url_for('admin.index')

        resp = self.google_oauth.authorized_response()

        try:
            if resp is None:
                raise AuthenticationError(
                    'Null response from Google, denying access.'
                )

            google_token = resp['access_token']

            username, email = self.get_google_user_profile_info(google_token)

            if not self.domain_check(email):
                return redirect(url_for('airflow.noaccess'))

        except AuthenticationError:
            return redirect(url_for('airflow.noaccess'))

        user = session.query(models.User).filter(
            models.User.username == username).first()

        if not user:
            user = models.User(
                username=username,
                email=email,
                is_superuser=False)

        session.merge(user)
        session.commit()
        login_user(GoogleUser(user))
        session.commit()

        return redirect(next_url)

login_manager = GoogleAuthBackend()
log.critical('GAB instantiated')


def login(self, request):
    log.critical('module login')
    return login_manager.login(request)

