import { useState } from 'react'

import {
  ControlOutlined,
  CopyOutlined,
  DatabaseOutlined,
  EyeOutlined,
  HomeOutlined,
  PartitionOutlined,
  ProjectOutlined,
  RocketOutlined
} from '@ant-design/icons'
import { Layout, Menu, MenuProps, Typography } from 'antd'

import { observer, useStore } from '@/hooks'

import VersionBar from './VersionBar'

import styles from './index.module.less'

type MenuItems = MenuProps['items']
export interface SiderMenuProps {
  collapsedWidth?: number
  siderWidth?: number
}

const { Title } = Typography
const { Sider } = Layout

const enableRBAC = window.environment?.enableRBAC
const showManagement = enableRBAC ? enableRBAC : process.env.REACT_APP_ENABLE_RBAC

const defaultProps = {
  collapsedWidth: 60,
  siderWidth: 200
}

const menuItems: MenuItems = [
  {
    key: 'home',
    icon: <HomeOutlined style={{ fontSize: '20px', color: '#e28743' }} />,
    label: '主页'
  },
  {
    key: 'projects',
    icon: <ProjectOutlined style={{ fontSize: '20px', color: '#177ddc' }} />,
    label: '项目'
  },
  {
    key: 'lineage',
    icon: <PartitionOutlined style={{ fontSize: '20px', color: '#b9038b' }} />,
    label: '血缘关系'
  },
  {
    key: 'datasources',
    icon: <DatabaseOutlined style={{ fontSize: '20px', color: '#13a8a8' }} />,
    label: '数据源'
  },
  {
    key: 'features',
    icon: <CopyOutlined style={{ fontSize: '20px', color: '#d89614' }} />,
    label: '特征'
  }
]

if (showManagement === 'true') {
  menuItems.push({
    key: 'management',
    icon: <ControlOutlined style={{ fontSize: '20px', color: '#6495ed' }} />,
    label: 'Management'
  })
}

const paths = ['lineage', 'datasources', 'features', 'jobs', 'monitoring']

const SideMenu = (props: SiderMenuProps) => {
  const { globalStore } = useStore()
  const { project, menuKeys, navigate, setSwitchProjecModalOpen } = globalStore

  const { siderWidth, collapsedWidth } = { ...defaultProps, ...props }

  const [collapsed] = useState<boolean>(false)

  const onClickMenu: MenuProps['onClick'] = (e) => {
    const { key } = e

    if (paths.includes(key)) {
      if (project) {
        navigate?.(`/${project}/${key}`)
      } else {
        setSwitchProjecModalOpen?.(true, key)
      }
    } else {
      navigate?.(`/${key}`)
    }
  }

  return (
    <>
      <div
        style={{
          width: collapsed ? collapsedWidth : siderWidth,
          overflow: 'hidden',
          flex: `0 0 ${collapsed ? collapsedWidth : siderWidth}px`,
          maxWidth: collapsed ? collapsedWidth : siderWidth,
          minWidth: collapsed ? collapsedWidth : siderWidth,
          transition: 'all 0.2s ease 0s'
        }}
      />
      <Sider className={styles.siderMenu} theme="dark" width={siderWidth}>
        <Title
          style={{
            fontSize: '36px',
            color: 'white',
            margin: '10px',
            paddingLeft: '35px'
          }}
          level={1}
        >
          Feathr
        </Title>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={menuKeys}
          items={menuItems}
          onClick={onClickMenu}
        />

        <VersionBar className={styles.versionBar} />
      </Sider>
    </>
  )
}

export default observer(SideMenu)
