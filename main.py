import subprocess,sys

def start(config):
    with open(config,'r') as f:
        cfg=yaml.load(f,Loader=yaml.CLoader)
    cfg['docker']['container']['services']['pipeline']['volumes'].append(cfg['users']+cfg['sharepoint']+":/home/pipeline/sharedata/")
    with open("pipeline.yaml",'w') as f:
        yaml.dump(cfg['docker']['container'],f,Dumper=yaml.CDumper)
    
    with open("Dockerfile","w") as f:
        f.write('\n\n'.join(cfg['docker']['image']))

if __name__ == '__main__':
    try:
        __import__('yaml')
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'PyYAML'])
    import yaml
    start("project.yaml")
    bashCommand = 'SSH_PRIVATE_KEY="$(cat ~/.ssh/id_ed25519)" docker-compose -f pipeline.yaml  up -d'
    process = subprocess.Popen(bashCommand,shell=True, stdout=subprocess.PIPE)
    output, error = process.communicate()
