import csv
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from services.client_service import normalize_phone_number


def deduplicate_contacts(contact_rows):
    seen = set()
    deduped = []

    for contact in contact_rows or []:
        name = str(contact.get('name', '')).strip() or 'UNKNOWN CONTACT'
        phone = normalize_phone_number(contact.get('phone', ''))
        label = str(contact.get('label', '')).strip()

        if not phone:
            continue

        key = (name.upper(), phone)
        if key in seen:
            continue

        seen.add(key)
        deduped.append({
            'name': name,
            'phone': phone,
            'label': label,
        })

    return sorted(deduped, key=lambda item: (item['name'].upper(), item['phone']))


def parse_contacts_csv(file_path):
    contacts = []

    with open(file_path, 'r', encoding='utf-8-sig', newline='') as csv_file:
        reader = csv.DictReader(csv_file)

        for row in reader:
            row = {key: (value or '').strip() for key, value in row.items() if key}
            name = (
                row.get('Name')
                or row.get('Full Name')
                or row.get('File As')
                or ' '.join(part for part in [row.get('Given Name', ''), row.get('Family Name', '')] if part).strip()
            )

            for key, value in row.items():
                key_lower = key.lower()
                if not value:
                    continue
                if 'phone' not in key_lower and 'mobile' not in key_lower and 'tel' not in key_lower:
                    continue
                if 'type' in key_lower:
                    continue

                phone = normalize_phone_number(value)
                if not phone:
                    continue

                label = key.replace(' - Value', '').replace('_', ' ').strip()
                contacts.append({
                    'name': name or phone,
                    'phone': phone,
                    'label': label,
                })

    return deduplicate_contacts(contacts)


def parse_contacts_vcf(file_path):
    contacts = []

    with open(file_path, 'r', encoding='utf-8', errors='ignore') as vcf_file:
        raw_lines = vcf_file.read().splitlines()

    lines = []
    for raw_line in raw_lines:
        if raw_line.startswith((' ', '\t')) and lines:
            lines[-1] += raw_line[1:]
        else:
            lines.append(raw_line.strip())

    current_name = ''
    current_numbers = []

    for line in lines:
        upper_line = line.upper()

        if upper_line == 'BEGIN:VCARD':
            current_name = ''
            current_numbers = []
            continue

        if upper_line == 'END:VCARD':
            for index, phone in enumerate(current_numbers, 1):
                contacts.append({
                    'name': current_name or phone,
                    'phone': phone,
                    'label': f'Phone {index}' if len(current_numbers) > 1 else 'Phone',
                })
            current_name = ''
            current_numbers = []
            continue

        if ':' not in line:
            continue

        field, value = line.split(':', 1)
        field_upper = field.upper()
        value = value.strip()

        if field_upper.startswith('FN') and value:
            current_name = value
        elif field_upper.startswith('N') and not current_name and value:
            parts = value.split(';')
            first_name = parts[1].strip() if len(parts) > 1 else ''
            last_name = parts[0].strip() if parts else ''
            current_name = ' '.join(part for part in [first_name, last_name] if part).strip()
        elif field_upper.startswith('TEL'):
            phone = normalize_phone_number(value)
            if phone:
                current_numbers.append(phone)

    return deduplicate_contacts(contacts)


def load_contacts_file(file_path):
    extension = os.path.splitext(file_path)[1].lower()

    if extension == '.csv':
        return parse_contacts_csv(file_path)
    if extension in {'.vcf', '.vcard'}:
        return parse_contacts_vcf(file_path)

    raise ValueError('Unsupported contact file. Use CSV or VCF.')


def fetch_google_contacts(oauth_file_path, token_file, scopes):
    oauth_file_path = str(oauth_file_path or '').strip()
    if not oauth_file_path:
        raise FileNotFoundError('Select your OAuth client JSON file first.')

    if not os.path.exists(oauth_file_path):
        raise FileNotFoundError(f'OAuth file not found: {oauth_file_path}')

    creds = None
    if os.path.exists(token_file):
        try:
            creds = UserCredentials.from_authorized_user_file(token_file, scopes)
        except Exception:
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(oauth_file_path, scopes)
            creds = flow.run_local_server(port=0)

        with open(token_file, 'w') as token_handle:
            token_handle.write(creds.to_json())

    service = build('people', 'v1', credentials=creds, cache_discovery=False)
    contacts = []
    page_token = None

    while True:
        response = service.people().connections().list(
            resourceName='people/me',
            pageSize=1000,
            pageToken=page_token,
            personFields='names,phoneNumbers',
        ).execute()

        for person in response.get('connections', []):
            names = person.get('names', [])
            display_name = names[0].get('displayName', '').strip() if names else ''

            for phone_info in person.get('phoneNumbers', []):
                phone = normalize_phone_number(phone_info.get('value', ''))
                if not phone:
                    continue

                label = phone_info.get('formattedType') or phone_info.get('type') or 'Phone'
                contacts.append({
                    'name': display_name or phone,
                    'phone': phone,
                    'label': label,
                })

        page_token = response.get('nextPageToken')
        if not page_token:
            break

    return deduplicate_contacts(contacts)