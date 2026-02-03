#!/usr/bin/env python3
"""
Create default admin account
"""
import pandas as pd
import boto3
import os
from io import BytesIO

def create_admin_account():
    bucket_name = os.getenv('AWS_BUCKET')
    
    if not bucket_name:
        print("❌ AWS_BUCKET environment variable not set")
        return False
    
    try:
        s3 = boto3.client('s3')
        
        # Create admin account
        admin_data = pd.DataFrame({
            "USERNAME": ["prince"],
            "PASSWORD": ["1234"],
            "ROLE": ["admin"],
            "APPROVED": ["YES"]
        })
        
        # Save to buffer
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            admin_data.to_excel(writer, index=False)
        
        excel_buffer.seek(0)
        
        # Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key='users/accounts.xlsx',
            Body=excel_buffer.getvalue(),
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
        print("✅ Created default admin account:")
        print("   Username: prince")
        print("   Password: 1234")
        print("   Role: admin")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating admin account: {e}")
        return False

if __name__ == '__main__':
    create_admin_account()
